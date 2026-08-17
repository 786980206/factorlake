#include "resolver/partition_resolver.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
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

static bool IsConstantDateFilter(const TableFilter &filter, ExpressionType &cmp, date_t &value) {
	if (filter.filter_type != TableFilterType::CONSTANT_COMPARISON) {
		return false;
	}
	auto &cf = filter.Cast<ConstantFilter>();
	if (cf.constant.IsNull() || cf.constant.type().id() != LogicalTypeId::DATE) {
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
			auto d = Date::FromString(value, false);
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
