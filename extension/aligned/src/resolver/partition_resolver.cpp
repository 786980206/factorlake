#include "resolver/partition_resolver.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

bool EvaluatePartitionTemplate(const string &template_str, date_t value, string &result) {
	int32_t year = Date::ExtractYear(value);
	int32_t month = Date::ExtractMonth(value);
	int32_t day = Date::ExtractDay(value);

	string out;
	out.reserve(template_str.size() + 8);
	bool has_specifier = false;
	for (idx_t i = 0; i < template_str.size(); i++) {
		char c = template_str[i];
		if (c != '%') {
			out += c;
			continue;
		}
		if (i + 1 >= template_str.size()) {
			return false; // trailing '%'
		}
		char spec = template_str[++i];
		switch (spec) {
		case 'Y':
			out += to_string(year);
			has_specifier = true;
			break;
		case 'm':
			if (month < 10) {
				out += '0';
			}
			out += to_string(month);
			has_specifier = true;
			break;
		case 'd':
			if (day < 10) {
				out += '0';
			}
			out += to_string(day);
			has_specifier = true;
			break;
		default:
			// Unsupported specifier: cannot evaluate this template
			return false;
		}
	}
	if (!has_specifier) {
		return false;
	}
	result = std::move(out);
	return true;
}

bool EvaluatePartitionTemplate(const string &template_str, int64_t value, string &result) {
	// int64_t key value can be either a date_t (for DATE columns) or a
	// timestamp_t (for TIMESTAMP columns). Convert to date_t for template
	// evaluation — the partition is always by date, not by timestamp.
	date_t d;
	if (value >= 0 && value <= 200000) {
		// Value is in date_t range (days since epoch: 0 = 1970-01-01, 200000 = ~2517 AD)
		d = date_t(static_cast<int32_t>(value));
	} else {
		// Value is a timestamp_t (microseconds since epoch)
		d = Timestamp::GetDate(timestamp_t(value));
	}
	return EvaluatePartitionTemplate(template_str, d, result);
}

bool IsKnownTemplate(const string &template_str) {
	return template_str == "date=%Y-%m-%d" || template_str == "month=%Y-%m" ||
	       template_str == "year=%Y";
}

string DefaultPartitionKey(const string &template_str) {
	if (template_str.rfind("date=", 0) == 0) {
		return "date=1970-01-01";
	} else if (template_str.rfind("month=", 0) == 0) {
		return "month=1970-01";
	} else if (template_str.rfind("year=", 0) == 0) {
		return "year=1970";
	}
	throw BinderException("aligned CREATE TABLE: invalid partition_template '%s' "
	                       "(expected 'date=%%Y-%%m-%%d', 'month=%%Y-%%m', or 'year=%%Y')",
	                       template_str);
}

void ValidatePartitionKey(const string &key, const string &template_str) {
	auto validate_date = [&](const string &date_str) {
		date_t result;
		idx_t pos = 0;
		bool special = false;
		auto rc = Date::TryConvertDate(date_str.c_str(), date_str.size(), pos, result,
		                                special, true /* strict */);
		if (rc != DateCastResult::SUCCESS) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' contains "
			                       "an invalid date '%s'", key, date_str);
		}
	};

	if (template_str.rfind("date=", 0) == 0) {
		if (key.rfind("date=", 0) != 0 || key.size() != 15) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' does not match "
			                       "template 'date=%%Y-%%m-%%d' (expected 'date=YYYY-MM-DD')", key);
		}
		validate_date(key.substr(5));
	} else if (template_str.rfind("month=", 0) == 0) {
		if (key.rfind("month=", 0) != 0 || key.size() != 13) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' does not match "
			                       "template 'month=%%Y-%%m' (expected 'month=YYYY-MM')", key);
		}
		validate_date(key.substr(6) + "-01");
	} else if (template_str.rfind("year=", 0) == 0) {
		if (key.rfind("year=", 0) != 0 || key.size() != 9) {
			throw BinderException("aligned CREATE TABLE: partition key '%s' does not match "
			                       "template 'year=%%Y' (expected 'year=YYYY')", key);
		}
		validate_date(key.substr(5) + "-01-01");
	} else {
		throw BinderException("aligned CREATE TABLE: invalid partition_template '%s'", template_str);
	}
}

namespace {

//! True when the range [from, to) of s contains only ASCII digits.
bool IsDigits(const string &s, size_t from, size_t to) {
	if (to > s.size() || from >= to) {
		return false;
	}
	for (size_t i = from; i < to; i++) {
		if (s[i] < '0' || s[i] > '9') {
			return false;
		}
	}
	return true;
}

//! Maps a partition segment value to its template ("" when not a recognized
//! format): "2026" -> "year=%Y", "2026-08" -> "month=%Y-%m",
//! "2026-08-17" -> "date=%Y-%m-%d".
string SegmentValueToTemplate(const string &value) {
	if (value.size() == 4 && IsDigits(value, 0, 4)) {
		return "year=%Y";
	}
	if (value.size() == 7 && value[4] == '-' && IsDigits(value, 0, 4) && IsDigits(value, 5, 7)) {
		return "month=%Y-%m";
	}
	if (value.size() == 10 && value[4] == '-' && value[7] == '-' && IsDigits(value, 0, 4) &&
	    IsDigits(value, 5, 7) && IsDigits(value, 8, 10)) {
		return "date=%Y-%m-%d";
	}
	return "";
}

} // namespace

vector<PartitionTemplate> DerivePartitioningFromPaths(const vector<string> &paths, const string &table_name,
                                                      const string &group_name, const string &source_column) {
	// Single-level partitioning (v5): the first path reveals the partition
	// segment; every other path must agree on the same format. The source
	// column (v6) is the index schema's DATE/TIMESTAMP field among its first
	// two columns — bound by the caller.
	vector<PartitionTemplate> result;
	for (auto &raw : paths) {
		string path = raw;
		std::replace(path.begin(), path.end(), '\\', '/');
		size_t pos = 0;
		while (pos < path.size()) {
			auto slash = path.find('/', pos);
			string segment = path.substr(pos, slash == string::npos ? string::npos : slash - pos);
			pos = slash == string::npos ? path.size() : slash + 1;
			auto eq = segment.find('=');
			if (eq == string::npos || eq == 0) {
				continue;
			}
			string name = segment.substr(0, eq);
			if (!StringUtil::CIEquals(name, "year") && !StringUtil::CIEquals(name, "month") &&
			    !StringUtil::CIEquals(name, "date")) {
				continue; // unknown partition kind: ignored
			}
			string tmpl = SegmentValueToTemplate(segment.substr(eq + 1));
			if (tmpl.empty()) {
				continue; // unrecognized value format: ignored
			}
			if (result.empty()) {
				PartitionTemplate t;
				t.template_str = tmpl;
				t.source = source_column.empty() ? "date" : source_column;
				result.push_back(std::move(t));
			} else if (result[0].template_str != tmpl) {
				throw IOException("Aligned table '%s' group '%s': partition segments have inconsistent formats "
				                  "('%s' vs '%s'); the v5 contract allows one single-level partition kind",
				                  table_name, group_name, result[0].template_str, tmpl);
			}
		}
	}
	return result;
}

static bool IsConstantDateFilter(const TableFilter &filter, ExpressionType &cmp, date_t &value) {
	if (filter.filter_type != TableFilterType::CONSTANT_COMPARISON) {
		return false;
	}
	auto &cf = filter.Cast<ConstantFilter>();
	if (cf.constant.IsNull()) {
		return false;
	}
	if (cf.constant.type().id() == LogicalTypeId::TIMESTAMP) {
		// v6: TIMESTAMP constants are compared on their date part (the
		// partition source column may be a TIMESTAMP field).
		cmp = cf.comparison_type;
		value = Timestamp::GetDate(cf.constant.GetValue<timestamp_t>());
		return true;
	}
	if (cf.constant.type().id() != LogicalTypeId::DATE) {
		return false;
	}
	cmp = cf.comparison_type;
	value = cf.constant.GetValue<date_t>();
	return true;
}

//! Reconstructs the partition date of a part from its directory values.
//! Every template must yield exactly one date component (%Y, %m, %d alone) or
//! the full date (%Y+%m+%d together). Returns false when not possible.
static bool ExtractPartitionDate(const PartInfo &part, const vector<PartitionTemplate> &templates, date_t &result) {
	string path = part.path;
	std::replace(path.begin(), path.end(), '\\', '/');

	int32_t year = 0;
	int32_t month = 0;
	int32_t day = 0;
	bool has_year = false;
	bool has_month = false;
	bool has_day = false;

	for (auto &t : templates) {
		auto eq = t.template_str.find('=');
		if (eq == string::npos) {
			return false;
		}
		string name = t.template_str.substr(0, eq);
		// find the directory component "name=value" in the part path
		string marker = "/" + name + "=";
		auto pos = path.find(marker);
		if (pos == string::npos) {
			return false;
		}
		auto value_start = pos + marker.size();
		auto value_end = path.find('/', value_start);
		string value = path.substr(value_start, value_end == string::npos ? string::npos : value_end - value_start);

		bool t_has_year = t.template_str.find("%Y") != string::npos;
		bool t_has_month = t.template_str.find("%m") != string::npos;
		bool t_has_day = t.template_str.find("%d") != string::npos;
		if (t_has_year && t_has_month && t_has_day) {
			// the value is a full date string (strftime output of the template)
			date_t d;
			try {
				d = Date::FromString(value, false);
			} catch (...) {
				return false; // malformed date in partition dir — skip pruning
			}
			if (d == date_t::infinity()) {
				return false;
			}
			result = d;
			return true;
		}
		idx_t components = (idx_t)t_has_year + (idx_t)t_has_month + (idx_t)t_has_day;
		if (components != 1) {
			return false; // partial date formats cannot be reconstructed
		}
		int32_t parsed = 0;
		try {
			parsed = std::stoi(value);
		} catch (...) {
			return false;
		}
		if (t_has_year) {
			year = parsed;
			has_year = true;
		} else if (t_has_month) {
			month = parsed;
			has_month = true;
		} else {
			day = parsed;
			has_day = true;
		}
	}
	if (!has_year || !has_month || !has_day) {
		return false; // e.g. month-level partitioning: the day is unknown
	}
	result = Date::FromDate(year, month, day);
	return result != date_t::infinity();
}

vector<PartInfo> PrunePartsByFilter(const vector<PartInfo> &parts, const vector<PartitionTemplate> &templates,
                                    const ConstantFilter &filter) {
	ExpressionType cmp;
	date_t value;
	if (!IsConstantDateFilter(filter, cmp, value)) {
		return parts; // not a date constant: no pruning
	}
	if (cmp == ExpressionType::COMPARE_NOTEQUAL) {
		return parts; // != cannot prune
	}

	// Verify all templates are evaluable first (with the reference value)
	vector<string> ref_dirs;
	for (auto &t : templates) {
		string dir;
		if (!EvaluatePartitionTemplate(t.template_str, value, dir)) {
			return parts; // unsupported template: no pruning
		}
		ref_dirs.push_back(std::move(dir));
	}

	if (cmp == ExpressionType::COMPARE_EQUAL) {
		// exact directory path match
		string dir_path;
		for (auto &d : ref_dirs) {
			if (!dir_path.empty()) {
				dir_path += "/";
			}
			dir_path += d;
		}
		vector<PartInfo> result;
		for (auto &part : parts) {
			string path = part.path;
			std::replace(path.begin(), path.end(), '\\', '/');
			if (StringUtil::Contains(path, "/" + dir_path + "/") || StringUtil::EndsWith(path, "/" + dir_path)) {
				result.push_back(part);
			}
		}
		return result;
	}

	// Range comparison: reconstruct each part's partition date and test it
	// (bounded by the number of parts — no unbounded date iteration)
	vector<PartInfo> result;
	for (auto &part : parts) {
		date_t part_date;
		if (!ExtractPartitionDate(part, templates, part_date)) {
			// date cannot be reconstructed (e.g. coarse partitioning):
			// keep the part — no pruning possible for this group
			return parts;
		}
		if (filter.Compare(Value::DATE(part_date))) {
			result.push_back(part);
		}
	}
	return result;
}

} // namespace duckdb
