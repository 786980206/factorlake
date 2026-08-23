"use strict";

var currentMode="browse";
var currentStep=0;       // 0 = not started
var stepFns=null;        // array of {text, apply} per mode

// ===== Mode switching =====
(function(){
  document.querySelectorAll(".mode-nav .nav-btn").forEach(function(btn){
    btn.addEventListener("click",function(){
      switchMode(btn.getAttribute("data-mode"));
    });
  });
})();

function switchMode(mode){
  currentMode=mode;
  resetAnim();
  document.querySelectorAll(".mode-nav .nav-btn").forEach(function(b){b.classList.remove("active")});
  document.querySelector('.mode-nav .nav-btn[data-mode="'+mode+'"]').classList.add("active");
  var tableBox=document.getElementById("tableBox");
  tableBox.classList.toggle("anim-mode",mode!=="browse");
  stepFns=buildSteps(mode);
  currentStep=0;
  updateStepUI();
}

// ===== Step engine =====
function nextStep(){
  if(!stepFns||currentStep>=stepFns.length)return;
  currentStep++;
  stepFns[currentStep-1].apply();
  updateStepUI();
}
function prevStep(){
  if(!stepFns||currentStep===0)return;
  currentStep--;
  if(currentStep===0){
    clearAllAnim();
    resetExpansion();
    showModeHint();
  }else{
    clearAllAnim();
    resetExpansion();
    for(var i=0;i<currentStep;i++)stepFns[i].apply();
  }
  updateStepUI();
}
function updateStepUI(){
  var total=stepFns?stepFns.length:0;
  document.getElementById("stepIndicator").innerHTML="<b>"+currentStep+"</b> / "+total;
  document.getElementById("prevBtn").disabled=(currentStep===0);
  document.getElementById("nextBtn").disabled=(currentStep>=total);
}

// ===== Reset =====
function resetAnim(){
  clearAllAnim();
  resetExpansion();
  currentStep=0;
  showModeHint();
  updateStepUI();
}

function showModeHint(){
  if(currentMode==="browse"){hideStepBar();return;}
  var hints={
    create:"✨ Create 模式：点击下一步开始演示建表流程",
    read:"🔍 Read 模式：点击下一步开始演示查询的 push down 和裁剪流程",
    update:"📝 Update 模式：点击下一步开始演示写入流程",
    delete:"🗑️ Delete 模式：点击下一步开始演示删除流程"
  };
  setStep("▶",hints[currentMode]);
}

function resetExpansion(){
  document.querySelectorAll(".partition.selected,.part.selected").forEach(function(el){
    el.classList.remove("selected");
  });
  if(currentMode==="browse"){
    expandPartition("index","2026-09");
    expandPartition("alpha101","2026-09");
    expandPartition("ma","2026-09");
  }
}

// ===== Step definitions per mode =====
function buildSteps(mode){
  if(mode==="read")return buildReadSteps();
  if(mode==="create")return buildCreateSteps();
  if(mode==="update")return buildUpdateSteps();
  if(mode==="delete")return buildDeleteSteps();
  return [];
}

// ──────────────────────────────────────────────
// READ: SELECT date, symbol, close, alpha001, ma20
//      FROM aligned_scan('cnstk_ixday')
//      WHERE date = '2026-09-01'
// ──────────────────────────────────────────────
function buildReadSteps(){
  return [
    {text:'<code>SELECT date, symbol, close, alpha001, ma20 FROM aligned_scan(\'cnstk_ixday\') WHERE date = \'2026-09-01\'</code>',
     apply:function(){
       setStep(1,this.text);
     }},

    {text:'<b>Projection Pushdown</b>：查询只需 5 个字段 → date, symbol, close, alpha001, ma20。底部字段栏高亮这 5 列（✓ 蓝色），其余列变灰。',
     apply:function(){
       setStep(2,this.text);
       addCls(selField("date"),"anim-proj");
       addCls(selField("symbol"),"anim-proj");
       addCls(selField("close"),"anim-proj");
       addCls(selField("alpha001"),"anim-proj");
       addCls(selField("ma20"),"anim-proj");
       ["volume","amount","alpha002","alpha101","alpha101b","ma5","ma10","ma60","ma120","ma250"].forEach(function(c){
         addCls(selField(c),"anim-dim");
       });
     }},

    {text:'<b>列组解析</b>：date/symbol/close → <b>index</b> 组；alpha001 → <b>alpha101</b> 组；ma20 → <b>ma</b> 组。只打开这 3 个列组（标 NEEDED），不打开其他组。',
     apply:function(){
       setStep(3,this.text);
       addCls('.group[data-g="index"]',"anim-active");
       addCls('.group[data-g="alpha101"]',"anim-active");
       addCls('.group[data-g="ma"]',"anim-active");
       expandPartition("index","2026-09");
       expandPartition("alpha101","2026-09");
       expandPartition("ma","2026-09");
     }},

    {text:'<b>Partition Pruning</b>：WHERE date=\'2026-09-01\' → 只需 <b>month=2026-09</b> 分区（标 READ 蓝色）。month=2026-07 被裁剪（标 SKIP 灰色），month=2026-10 为空也跳过。',
     apply:function(){
       setStep(4,this.text);
       addCls(selPartition("index","2026-09"),"anim-hit");
       addCls(selPartition("alpha101","2026-09"),"anim-hit");
       addCls(selPartition("ma","2026-09"),"anim-hit");
       addCls(selPartition("index","2026-07"),"anim-skip");
       addCls(selPartition("alpha101","2026-07"),"anim-skip");
       addCls(selPartition("ma","2026-07"),"anim-skip");
       addCls(selPartition("index","2026-10"),"anim-skip");
       addCls(selPartition("alpha101","2026-10"),"anim-skip");
       addCls(selPartition("ma","2026-10"),"anim-skip");
     }},

    {text:'<b>Row Group Pruning</b>：打开 0000-0000005998.parquet，用 RG 统计信息 (min/max date) 跳过不含 2026-09-01 的 RG。只有 RG-B 命中（标 READ），RG-A / RG-C 被裁剪（标 SKIP）。',
     apply:function(){
       setStep(5,this.text);
       expandPart("index","2026-09","0000");
       expandPart("alpha101","2026-09","0000");
       expandPart("ma","2026-09","0000");
       addCls(selPart("index","2026-09","0000"),"anim-hit");
       addCls(selPart("alpha101","2026-09","0000"),"anim-hit");
       addCls(selPart("ma","2026-09","0000"),"anim-hit");
       addCls(selRG("index","2026-09","0000","B"),"anim-hit");
       addCls(selRG("alpha101","2026-09","0000","B"),"anim-hit");
       addCls(selRG("ma","2026-09","0000","B"),"anim-hit");
       addCls(selRG("index","2026-09","0000","A"),"anim-skip");
       addCls(selRG("index","2026-09","0000","C"),"anim-skip");
       addCls(selRG("alpha101","2026-09","0000","A"),"anim-skip");
       addCls(selRG("ma","2026-09","0000","A"),"anim-skip");
     }},

    {text:'<b>Column Projection</b>：在命中的 RG-B 内，只读 date/symbol/close（index 组）、alpha001（alpha101 组）、ma20（ma 组）。其余列不读（变灰）。',
     apply:function(){
       setStep(6,this.text);
       highlightColChips("index","2026-09","0000","B",["date","symbol","close"]);
       highlightColChips("alpha101","2026-09","0000","B",["alpha001"]);
       highlightColChips("ma","2026-09","0000","B",["ma20"]);
     }},

    {text:'<b>DataChunk 组装</b>：三个 Parquet Reader 的 Vector 直接填充同一个 DataChunk 的对应列。不做 JOIN，position-aligned 对齐。缺分区/缺列 NULL 填充。',
     apply:function(){
       setStep(7,this.text);
     }},
  ];
}

// ──────────────────────────────────────────────
// CREATE
// ──────────────────────────────────────────────
function buildCreateSteps(){
  return [
    {text:'<code>aligned_create(\'cnstk_ixday\', \'index\', \'symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT, amount DOUBLE\')</code> — 创建表目录 + index 列组',
     apply:function(){
       setStep(1,this.text);
       addCls('.group[data-g="index"]',"anim-active");
     }},

    {text:'创建默认分区 <code>month=1970-01</code> + 0 行占位 parquet（footer 携带完整 schema，作为 Canonical Key）',
     apply:function(){
       setStep(2,this.text);
     }},

    {text:'<code>aligned_create(\'cnstk_ixday\', \'factor/alpha101\', \'alpha001 DOUBLE, alpha002 DOUBLE, ...\')</code> — 扩展第二个列组',
     apply:function(){
       setStep(3,this.text);
       addCls('.group[data-g="alpha101"]',"anim-active");
       expandPartition("alpha101","2026-07");
       expandPartition("alpha101","2026-09");
       addCls(selPartition("alpha101","2026-07"),"anim-new");
       addCls(selPartition("alpha101","2026-09"),"anim-new");
     }},

    {text:'为新列组的每个已有分区写 N 行全 NULL 占位 parquet — N = index 分区行数，满足分区对齐契约',
     apply:function(){
       setStep(4,this.text);
     }},

    {text:'<code>aligned_create(\'cnstk_ixday\', \'fieldset/ma\', \'ma5 DOUBLE, ma20 DOUBLE, ...\')</code> — 再扩展一个列组',
     apply:function(){
       setStep(5,this.text);
       addCls('.group[data-g="ma"]',"anim-active");
       expandPartition("ma","2026-07");
       expandPartition("ma","2026-09");
       addCls(selPartition("ma","2026-07"),"anim-new");
       addCls(selPartition("ma","2026-09"),"anim-new");
     }},

    {text:'✅ 建表完成！表目录结构就绪，后续 INSERT 会创建真实分区并写入数据。',
     apply:function(){
       setStep(6,this.text);
     }},
  ];
}

// ──────────────────────────────────────────────
// UPDATE
// ──────────────────────────────────────────────
function buildUpdateSteps(){
  return [
    {text:'<code>INSERT INTO al.cnstk_ixday VALUES (\'600000\', DATE \'2026-10-15\', 12.5, 1000, 500, 0.3, 0.5, 20.0)</code>',
     apply:function(){
       setStep(1,this.text);
       addCls('.group[data-g="index"]',"anim-active");
       addCls('.group[data-g="alpha101"]',"anim-active");
       addCls('.group[data-g="ma"]',"anim-active");
     }},

    {text:'<b>主键定位</b>：(symbol=\'600000\', date=\'2026-10-15\') → 分区 <b>month=2026-10</b>。该分区当前为空（无 part）。',
     apply:function(){
       setStep(2,this.text);
       expandPartition("index","2026-10");
       addCls(selPartition("index","2026-10"),"anim-hit");
     }},

    {text:'<b>分区不存在 → 创建新分区</b>：在所有列组创建 month=2026-10/ 目录（标 NEW 绿色）',
     apply:function(){
       setStep(3,this.text);
       expandPartition("alpha101","2026-10");
       expandPartition("ma","2026-10");
       addCls(selPartition("alpha101","2026-10"),"anim-new");
       addCls(selPartition("ma","2026-10"),"anim-new");
     }},

    {text:'<b>写入 index 列组</b>：新建 part 0000，写入 1 行 (symbol, date, close, volume, amount)（标 WRITE 绿色脉冲）',
     apply:function(){
       setStep(4,this.text);
       expandPart("index","2026-10","0000");
       addCls(selPart("index","2026-10","0000"),"anim-writing");
     }},

    {text:'<b>写入 alpha101 列组</b>：新建 part 0000，写入 1 行 (alpha001=0.3, alpha002=0.5)',
     apply:function(){
       setStep(5,this.text);
       expandPart("alpha101","2026-10","0000");
       addCls(selPart("alpha101","2026-10","0000"),"anim-writing");
     }},

    {text:'<b>写入 ma 列组</b>：新建 part 0000，写入 1 行 (ma20=20.0，其余 ma 列 NULL)',
     apply:function(){
       setStep(6,this.text);
       expandPart("ma","2026-10","0000");
       addCls(selPart("ma","2026-10","0000"),"anim-writing");
     }},

    {text:'<b>两阶段提交</b>：先写 <code>_tmp/transaction-42/</code> → 原子 move 到正式目录 → 提交完成（标 READ 蓝色表示已持久化）',
     apply:function(){
       setStep(7,this.text);
       rmCls(selPart("index","2026-10","0000"),"anim-writing");
       rmCls(selPart("alpha101","2026-10","0000"),"anim-writing");
       rmCls(selPart("ma","2026-10","0000"),"anim-writing");
       addCls(selPart("index","2026-10","0000"),"anim-hit");
       addCls(selPart("alpha101","2026-10","0000"),"anim-hit");
       addCls(selPart("ma","2026-10","0000"),"anim-hit");
     }},

    {text:'✅ 写入完成！数据已持久化，三个列组的 month=2026-10 分区各有一个 part。',
     apply:function(){
       setStep(8,this.text);
     }},
  ];
}

// ──────────────────────────────────────────────
// DELETE
// ──────────────────────────────────────────────
function buildDeleteSteps(){
  return [
    {text:'<code>aligned_drop(\'cnstk_ixday\', \'factor/alpha101\')</code> — 删除单个列组',
     apply:function(){
       setStep(1,this.text);
       addCls('.group[data-g="alpha101"]',"anim-deleting");
     }},

    {text:'<b>获取表级写锁</b> <code>.aligned_write.lock</code> — 阻止并发写入',
     apply:function(){
       setStep(2,this.text);
     }},

    {text:'<b>递归删除</b> alpha101/ 整个目录树 — 所有分区、所有 part 文件（标 DELETE 红色抖动）',
     apply:function(){
       setStep(3,this.text);
       expandPartition("alpha101","2026-07");
       expandPartition("alpha101","2026-09");
       addCls(selPartition("alpha101","2026-07"),"anim-deleting");
       addCls(selPartition("alpha101","2026-09"),"anim-deleting");
       addCls(selPartition("alpha101","2026-10"),"anim-deleting");
     }},

    {text:'alpha101 组从磁盘消失 — index 和 ma 不受影响',
     apply:function(){
       setStep(4,this.text);
       addCls(selPartition("alpha101","2026-07"),"anim-gone");
       addCls(selPartition("alpha101","2026-09"),"anim-gone");
       addCls(selPartition("alpha101","2026-10"),"anim-gone");
       addCls('.group[data-g="alpha101"]',"anim-gone");
     }},

    {text:'<code>aligned_drop(\'cnstk_ixday\', \'index\')</code> — 删除整张表',
     apply:function(){
       setStep(5,this.text);
       addCls('.group[data-g="index"]',"anim-deleting");
       addCls('.group[data-g="ma"]',"anim-deleting");
     }},

    {text:'<b>删除整个表目录</b> cnstk_ixday/ — 所有列组、所有分区、所有文件',
     apply:function(){
       setStep(6,this.text);
       document.querySelectorAll(".group:not(.group-more)").forEach(function(el){
         el.classList.add("anim-deleting");
         setTimeout(function(){el.classList.add("anim-gone");},300);
       });
     }},

    {text:'✅ 删除完成！整张表目录已从磁盘移除。',
     apply:function(){
       setStep(7,this.text);
     }},
  ];
}

// ===== Helper: highlight specific col-chips inside an RG =====
function highlightColChips(group,p,part,rg,colNames){
  var rgEl=document.querySelector(selRG(group,p,part,rg));
  if(!rgEl)return;
  rgEl.querySelectorAll(".col-chip").forEach(function(chip){
    var text=chip.textContent.trim();
    if(colNames.indexOf(text)>=0){
      chip.classList.add("anim-proj");
    }else{
      chip.style.opacity="0.2";
      chip.style.filter="grayscale(.8)";
    }
  });
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

// ===== Keyboard navigation =====
document.addEventListener("keydown",function(e){
  if(currentMode==="browse")return;
  if(e.key==="ArrowRight"||e.key===" ")nextStep();
  else if(e.key==="ArrowLeft")prevStep();
});

// ===== Init =====
switchMode("browse");
