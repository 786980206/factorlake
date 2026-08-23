"use strict";

var currentMode="browse";
var timers=[];

// ===== Mode switching =====
(function(){
  document.querySelectorAll(".mode-nav .nav-btn").forEach(function(btn){
    btn.addEventListener("click",function(){
      var mode=btn.getAttribute("data-mode");
      switchMode(mode);
    });
  });
})();

function switchMode(mode){
  clearAllTimers();
  resetAnim();
  currentMode=mode;
  document.querySelectorAll(".mode-nav .nav-btn").forEach(function(b){b.classList.remove("active")});
  document.querySelector('.mode-nav .nav-btn[data-mode="'+mode+'"]').classList.add("active");
  var tableBox=document.getElementById("tableBox");
  if(mode==="browse"){
    tableBox.classList.remove("anim-mode");
    hideStepBar();
  }else{
    tableBox.classList.add("anim-mode");
    var hints={
      create:"✨ Create 模式：演示 aligned_create 建表 + 扩展列组流程",
      read:"🔍 Read 模式：演示 SELECT 查询的 Projection Pushdown + 分区裁剪 + RG 裁剪",
      update:"📝 Update 模式：演示 INSERT 按 (symbol,date) 主键写入 + 两阶段提交",
      delete:"🗑️ Delete 模式：演示 aligned_drop 删除列组 + 删除整表"
    };
    setStep("▶",hints[mode]);
  }
}

// ===== Timer management =====
function setTimeout2(fn,delay){
  var t=setTimeout(fn,delay);
  timers.push(t);
  return t;
}
function clearAllTimers(){
  timers.forEach(function(t){clearTimeout(t);});
  timers=[];
}

function playAnim(){
  clearAllTimers();
  resetAnim();
  if(currentMode==="create")playCreate();
  else if(currentMode==="read")playRead();
  else if(currentMode==="update")playUpdate();
  else if(currentMode==="delete")playDelete();
}

function resetAnim(){
  clearAllTimers();
  clearAllAnim();
  // Collapse all expanded partitions/parts (remove "selected" added by animation)
  document.querySelectorAll(".partition.selected,.part.selected").forEach(function(el){
    el.classList.remove("selected");
  });
  // Collapse everything back to default
  document.querySelectorAll(".partition.selected").forEach(function(el){el.classList.remove("selected")});
  // Re-expand the default (2026-09) partitions for browse mode
  if(currentMode==="browse"||currentMode==="read"){
    expandPartition("index","2026-09");
    expandPartition("alpha101","2026-09");
    expandPartition("ma","2026-09");
  }
  var tableBox=document.getElementById("tableBox");
  tableBox.classList.remove("has-sel","has-sel-part","has-sel-rg");
  if(currentMode!=="browse"){
    var hints={
      create:"✨ Create 模式：演示 aligned_create 建表 + 扩展列组流程",
      read:"🔍 Read 模式：演示 SELECT 查询的 Projection Pushdown + 分区裁剪 + RG 裁剪",
      update:"📝 Update 模式：演示 INSERT 按 (symbol,date) 主键写入 + 两阶段提交",
      delete:"🗑️ Delete 模式：演示 aligned_drop 删除列组 + 删除整表"
    };
    setStep("▶",hints[currentMode]);
  }
}

// ===== READ animation =====
function playRead(){
  var tableBox=document.getElementById("tableBox");
  tableBox.classList.add("anim-mode");

  // Step 1: Show the query
  setTimeout2(function(){
    setStep(1,'<code>SELECT date, symbol, close, alpha001, ma20 FROM aligned_scan(\'cnstk_ixday\') WHERE date = \'2026-09-01\'</code>');
  },200);

  // Step 2: Projection Pushdown — highlight needed columns
  setTimeout2(function(){
    setStep(2,"Projection Pushdown：只读需要的列 — 高亮 date, symbol, close, alpha001, ma20");
    addCls(selField("date"),"anim-proj");
    addCls(selField("symbol"),"anim-proj");
    addCls(selField("close"),"anim-proj");
    addCls(selField("alpha001"),"anim-proj");
    addCls(selField("ma20"),"anim-proj");
  },2000);

  // Step 3: Group resolution — highlight the 3 groups needed
  setTimeout2(function(){
    setStep(3,"列组解析：date/symbol/close → index 组，alpha001 → alpha101 组，ma20 → ma 组");
    addCls('.group[data-g="index"]',"anim-active");
    addCls('.group[data-g="alpha101"]',"anim-active");
    addCls('.group[data-g="ma"]',"anim-active");
  },4000);

  // Step 4: Partition Pruning — WHERE date='2026-09-01' → only month=2026-09
  setTimeout2(function(){
    setStep(4,"分区裁剪 (Partition Pruning)：WHERE date='2026-09-01' → 只打开 month=2026-09 分区，跳过 month=2026-07");
    // Expand 2026-09 in all groups
    expandPartition("index","2026-09");
    expandPartition("alpha101","2026-09");
    expandPartition("ma","2026-09");
    // Mark 2026-09 as hit
    addCls(selPartition("index","2026-09"),"anim-hit");
    addCls(selPartition("alpha101","2026-09"),"anim-hit");
    addCls(selPartition("ma","2026-09"),"anim-hit");
    // Mark 2026-07 as skipped
    addCls(selPartition("index","2026-07"),"anim-skip");
    addCls(selPartition("alpha101","2026-07"),"anim-skip");
    addCls(selPartition("ma","2026-07"),"anim-skip");
    // Mark 2026-10 (empty) as skipped too
    addCls(selPartition("index","2026-10"),"anim-skip");
    addCls(selPartition("alpha101","2026-10"),"anim-skip");
    addCls(selPartition("ma","2026-10"),"anim-skip");
  },6000);

  // Step 5: Row Group Pruning — open parquet, use RG stats to skip RGs
  setTimeout2(function(){
    setStep(5,"Row Group 裁剪：打开 2026-09 的 part，用 RG 统计信息 (min/max date) 跳过不包含 '2026-09-01' 的 RG");
    expandPart("index","2026-09","0000");
    expandPart("alpha101","2026-09","0000");
    expandPart("ma","2026-09","0000");
    addCls(selPart("index","2026-09","0000"),"anim-hit");
    addCls(selPart("alpha101","2026-09","0000"),"anim-hit");
    addCls(selPart("ma","2026-09","0000"),"anim-hit");
    // Highlight specific RGs
    addCls(selRG("index","2026-09","0000","B"),"anim-hit");
    addCls(selRG("alpha101","2026-09","0000","B"),"anim-hit");
    addCls(selRG("ma","2026-09","0000","B"),"anim-hit");
  },8000);

  // Step 6: DataChunk assembly
  setTimeout2(function(){
    setStep(6,"DataChunk 组装：三个 Parquet Reader 的 Vector 直接填充同一行 — 不做 JOIN，position-aligned 对齐");
  },10000);

  // Done
  setTimeout2(function(){
    setStep("✅","查询完成！只读了 3 个列组 × 1 个分区 × 1 个 part 中的部分 RG，零 JOIN。");
  },12000);
}

// ===== CREATE animation =====
function playCreate(){
  // Step 1: Create table directory
  setTimeout2(function(){
    setStep(1,"<code>aligned_create('cnstk_ixday','index','symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT, amount DOUBLE')</code>");
    addCls('.group[data-g="index"]',"anim-active");
  },200);

  // Step 2: Create default partition
  setTimeout2(function(){
    setStep(2,"创建默认分区 <code>month=1970-01</code> + 0 行占位 parquet（footer 携带完整 schema）");
  },2000);

  // Step 3: Extend with alpha101 group
  setTimeout2(function(){
    setStep(3,"<code>aligned_create('cnstk_ixday','factor/alpha101','alpha001 DOUBLE, alpha002 DOUBLE, ...')</code> — 扩展列组");
    addCls('.group[data-g="alpha101"]',"anim-active");
  },4000);

  // Step 4: NULL placeholder for existing partitions
  setTimeout2(function(){
    setStep(4,"为新列组的每个已有分区写 N 行全 NULL 占位 parquet — 满足分区对齐契约");
    expandPartition("alpha101","2026-07");
    expandPartition("alpha101","2026-09");
    addCls(selPartition("alpha101","2026-07"),"anim-new");
    addCls(selPartition("alpha101","2026-09"),"anim-new");
  },6000);

  // Step 5: Extend with ma group
  setTimeout2(function(){
    setStep(5,"<code>aligned_create('cnstk_ixday','fieldset/ma','ma5 DOUBLE, ma10 DOUBLE, ...')</code> — 再扩展一个列组");
    addCls('.group[data-g="ma"]',"anim-active");
    expandPartition("ma","2026-07");
    expandPartition("ma","2026-09");
    addCls(selPartition("ma","2026-07"),"anim-new");
    addCls(selPartition("ma","2026-09"),"anim-new");
  },8000);

  // Done
  setTimeout2(function(){
    setStep("✅","建表完成！表目录结构就绪，后续 INSERT 会创建真实分区并写入数据。");
  },10000);
}

// ===== UPDATE animation =====
function playUpdate(){
  // Step 1: INSERT statement
  setTimeout2(function(){
    setStep(1,"<code>INSERT INTO al.cnstk_ixday VALUES ('600000', DATE '2026-10-15', 12.5, 1000, 500, 0.3, 0.5, 20.0)</code>");
  },200);

  // Step 2: Key resolution
  setTimeout2(function(){
    setStep(2,"主键定位：(symbol='600000', date='2026-10-15') → 分区 month=2026-10");
    // Highlight 2026-10 partition (currently empty)
    expandPartition("index","2026-10");
    addCls(selPartition("index","2026-10"),"anim-hit");
  },2000);

  // Step 3: Create new partition in all groups
  setTimeout2(function(){
    setStep(3,"分区不存在 → 在所有列组创建新分区 month=2026-10/");
    expandPartition("alpha101","2026-10");
    expandPartition("ma","2026-10");
    addCls(selPartition("alpha101","2026-10"),"anim-new");
    addCls(selPartition("ma","2026-10"),"anim-new");
  },4000);

  // Step 4: Write index group
  setTimeout2(function(){
    setStep(4,"写入 index 列组 — 新建 part 0000，包含 1 行数据");
    expandPart("index","2026-10","0000");
    addCls(selPart("index","2026-10","0000"),"anim-writing");
  },6000);

  // Step 5: Write alpha101 group
  setTimeout2(function(){
    setStep(5,"写入 alpha101 列组 — 新建 part 0000");
    expandPart("alpha101","2026-10","0000");
    addCls(selPart("alpha101","2026-10","0000"),"anim-writing");
  },8000);

  // Step 6: Write ma group
  setTimeout2(function(){
    setStep(6,"写入 ma 列组 — 新建 part 0000");
    expandPart("ma","2026-10","0000");
    addCls(selPart("ma","2026-10","0000"),"anim-writing");
  },10000);

  // Step 7: Two-phase commit
  setTimeout2(function(){
    setStep(7,"两阶段提交：先写 <code>_tmp/transaction-42/</code> → 原子 move 到正式目录 → 提交完成");
    rmCls(selPart("index","2026-10","0000"),"anim-writing");
    rmCls(selPart("alpha101","2026-10","0000"),"anim-writing");
    rmCls(selPart("ma","2026-10","0000"),"anim-writing");
    addCls(selPart("index","2026-10","0000"),"anim-hit");
    addCls(selPart("alpha101","2026-10","0000"),"anim-hit");
    addCls(selPart("ma","2026-10","0000"),"anim-hit");
  },12000);

  // Done
  setTimeout2(function(){
    setStep("✅","写入完成！数据已持久化，三个列组的 month=2026-10 分区各有一个 part。");
  },14000);
}

// ===== DELETE animation =====
function playDelete(){
  // Step 1: Drop a column group
  setTimeout2(function(){
    setStep(1,"<code>aligned_drop('cnstk_ixday', 'factor/alpha101')</code> — 删除单个列组");
    addCls('.group[data-g="alpha101"]',"anim-deleting");
  },200);

  // Step 2: Write lock
  setTimeout2(function(){
    setStep(2,"获取表级写锁 <code>.aligned_write.lock</code> — 阻止并发写入");
  },2000);

  // Step 3: Recursive delete of group directory
  setTimeout2(function(){
    setStep(3,"递归删除 alpha101/ 整个目录树 — 所有分区、所有 part 文件");
    expandPartition("alpha101","2026-07");
    expandPartition("alpha101","2026-09");
    addCls(selPartition("alpha101","2026-07"),"anim-deleting");
    addCls(selPartition("alpha101","2026-09"),"anim-deleting");
  },4000);

  // Step 4: Fade out deleted partitions
  setTimeout2(function(){
    setStep(4,"删除完成 — alpha101 组从磁盘消失，index 和 ma 不受影响");
    addCls(selPartition("alpha101","2026-07"),"anim-gone");
    addCls(selPartition("alpha101","2026-09"),"anim-gone");
    addCls(selPartition("alpha101","2026-10"),"anim-gone");
    addCls('.group[data-g="alpha101"]',"anim-gone");
  },6000);

  // Step 5: Drop the entire table
  setTimeout2(function(){
    setStep(5,"<code>aligned_drop('cnstk_ixday', 'index')</code> — 删除整张表");
    addCls('.group[data-g="index"]',"anim-deleting");
    addCls('.group[data-g="ma"]',"anim-deleting");
  },8000);

  // Step 6: Delete everything
  setTimeout2(function(){
    setStep(6,"删除整个表目录 cnstk_ixday/ — 所有列组、所有分区、所有文件");
    document.querySelectorAll(".group:not(.group-more)").forEach(function(el){
      el.classList.add("anim-deleting");
      setTimeout(function(){el.classList.add("anim-gone");},500);
    });
  },10000);

  // Done
  setTimeout2(function(){
    setStep("✅","删除完成！整张表目录已从磁盘移除。");
  },12000);
}

// ===== Browse mode: original click interactions =====
(function(){
  var tableBox=document.getElementById("tableBox");
  if(!tableBox)return;

  document.querySelectorAll(".partition-header").forEach(function(h){
    h.addEventListener("click",function(e){
      if(currentMode!=="browse")return;
      e.stopPropagation();
      var p=h.closest(".partition"),key=p.getAttribute("data-p");
      var isOn=p.classList.contains("selected");
      document.querySelectorAll('.partition[data-p="'+key+'"]').forEach(function(x){
        if(isOn)x.classList.remove("selected");else x.classList.add("selected");
      });
      if(isOn){
        document.querySelectorAll('.partition[data-p="'+key+'"] .part.selected').forEach(function(x){x.classList.remove("selected")});
        document.querySelectorAll('.partition[data-p="'+key+'"] .rg.selected').forEach(function(x){x.classList.remove("selected")});
      }
      updateSpotlight();
    });
  });

  document.querySelectorAll(".part-header").forEach(function(h){
    h.addEventListener("click",function(e){
      if(currentMode!=="browse")return;
      e.stopPropagation();
      var pt=h.closest(".part"),pKey=pt.closest(".partition").getAttribute("data-p"),partKey=pt.getAttribute("data-part");
      if(!pt.closest(".partition").classList.contains("selected")){
        document.querySelectorAll('.partition[data-p="'+pKey+'"]').forEach(function(x){x.classList.add("selected")});
      }
      var isOn=pt.classList.contains("selected");
      document.querySelectorAll('.partition[data-p="'+pKey+'"] .part[data-part="'+partKey+'"]').forEach(function(x){
        if(isOn)x.classList.remove("selected");else x.classList.add("selected");
      });
      if(isOn){
        document.querySelectorAll('.partition[data-p="'+pKey+'"] .part[data-part="'+partKey+'"] .rg.selected').forEach(function(x){x.classList.remove("selected")});
      }
      updateSpotlight();
    });
  });

  document.querySelectorAll(".rg-label").forEach(function(h){
    h.addEventListener("click",function(e){
      if(currentMode!=="browse")return;
      e.stopPropagation();
      var rg=h.closest(".rg"),rgKey=rg.getAttribute("data-rg"),pKey=rg.closest(".partition").getAttribute("data-p"),partKey=rg.closest(".part").getAttribute("data-part");
      var isOn=rg.classList.contains("selected");
      document.querySelectorAll('.partition[data-p="'+pKey+'"] .part[data-part="'+partKey+'"] .rg[data-rg="'+rgKey+'"]').forEach(function(x){
        if(isOn)x.classList.remove("selected");else x.classList.add("selected");
      });
      updateSpotlight();
    });
  });

  function updateSpotlight(){
    if(currentMode!=="browse")return;
    var hasP=document.querySelector(".partition.selected"),hasPt=document.querySelector(".part.selected"),hasRG=document.querySelector(".rg.selected");
    tableBox.classList.remove("has-sel","has-sel-part","has-sel-rg");
    if(hasRG)tableBox.classList.add("has-sel","has-sel-part","has-sel-rg");
    else if(hasPt)tableBox.classList.add("has-sel","has-sel-part");
    else if(hasP)tableBox.classList.add("has-sel");
  }
})();

// Expand default partitions on load
expandPartition("index","2026-09");
expandPartition("alpha101","2026-09");
expandPartition("ma","2026-09");
