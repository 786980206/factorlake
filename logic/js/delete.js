"use strict";
// DELETE animation: aligned_drop / DELETE — 删除列组或行

var deleteSteps=[
  {step:1,text:"步骤 1/6: <code>aligned_drop('cnstk_ixday', 'factor/alpha101')</code> — 删除单个列组"},
  {step:2,text:"步骤 2/6: 获取表级写锁 <code>.aligned_write.lock</code> — 阻止并发写入"},
  {step:3,text:"步骤 3/6: 定位 factor/alpha101 列组目录，递归计算文件数和目录数"},
  {step:4,text:"步骤 4/6: 删除 factor/alpha101/ 整个目录树 — 所有分区、所有 part 文件"},
  {step:5,text:"步骤 5/6: <code>aligned_drop('cnstk_ixday', 'index')</code> — 删除整张表"},
  {step:6,text:"步骤 6/6: 删除整个表目录 cnstk_ixday/ — 所有列组、所有分区、所有文件"},
];
var deleteNodes=[],deleteCanvas;

function initDelete(){
  deleteCanvas=document.getElementById("delete-canvas");
  deleteNodes=[
    makeNode("group-index","🔑 index",10,10,70,28),
    makeNode("partition","month=2026-07",14,42,110,24),
    makeNode("partition","month=2026-09",14,72,110,24),
    makeNode("group-factor","🧮 alpha101",38,10,90,28),
    makeNode("partition","month=2026-07",42,42,110,24),
    makeNode("partition","month=2026-09",42,72,110,24),
    makeNode("group-ma","📐 ma",66,10,70,28),
    makeNode("partition","month=2026-07",70,42,110,24),
    makeNode("partition","month=2026-09",70,72,110,24),
  ];
  renderNodes(deleteCanvas,deleteNodes);
  setStepText(deleteCanvas,"点击 ▶ 播放 开始演示删除流程");
}

function playDelete(){
  resetCRUD("delete");
  var stepEl=document.getElementById("delete-step");
  stepEl.innerHTML="步骤 <b>1</b> / 6";
  deleteNodes.forEach(function(n,i){showNode(n,i*60);});
  setStepText(deleteCanvas,deleteSteps[0].text);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>2</b> / 6";
    setStepText(deleteCanvas,deleteSteps[1].text);
  },1500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>3</b> / 6";
    highlightNode(deleteNodes[3]);
    setStepText(deleteCanvas,deleteSteps[2].text);
  },3000);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>4</b> / 6";
    [deleteNodes[3],deleteNodes[4],deleteNodes[5]].forEach(function(n){
      deletingNode(n);
      setTimeout(function(){fadeOutNode(n);},800);
    });
    setStepText(deleteCanvas,deleteSteps[3].text);
  },4500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>5</b> / 6";
    setStepText(deleteCanvas,deleteSteps[4].text);
  },6500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>6</b> / 6";
    deleteNodes.forEach(function(n){
      if(n.el&&!n.el.classList.contains("fade-out")){
        deletingNode(n);
        setTimeout(function(){fadeOutNode(n);},600);
      }
    });
    setStepText(deleteCanvas,deleteSteps[5].text);
  },8000);

  setTimeout(function(){
    setStepText(deleteCanvas,"✅ 删除完成！整张表目录已从磁盘移除。");
  },10500);
}

// ===== CRUD controller (shared) =====
function playCRUD(type){
  if(type==="create")playCreate();
  else if(type==="read")playRead();
  else if(type==="update")playUpdate();
  else if(type==="delete")playDelete();
}

function resetCRUD(type){
  var canvas=document.getElementById(type+"-canvas");
  if(!canvas)return;
  canvas.innerHTML="";
  if(type==="create")initCreate();
  else if(type==="read")initRead();
  else if(type==="update")initUpdate();
  else if(type==="delete")initDelete();
  var totalSteps={create:5,read:6,update:7,delete:6};
  document.getElementById(type+"-step").innerHTML="步骤 0 / "+totalSteps[type];
}

// Initialize all CRUD canvases on load
initCreate();initRead();initUpdate();initDelete();
