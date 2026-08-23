"use strict";
// UPDATE animation: INSERT / UPDATE — 按 (symbol, date) 主键写入

var updateSteps=[
  {step:1,text:"步骤 1/7: <code>INSERT INTO al.cnstk_ixday VALUES ('600000', DATE '2026-10-15', 12.5, 1000, 500, 0.3, 0.5, 20.0)</code>"},
  {step:2,text:"步骤 2/7: 主键定位 — (symbol='600000', date='2026-10-15') → 分区 month=2026-10"},
  {step:3,text:"步骤 3/7: 分区不存在 → 创建新分区目录 month=2026-10/，同时在所有列组创建分区"},
  {step:4,text:"步骤 4/7: 写入 index 列组 — 新建 part 0000，包含 1 行数据（symbol,date,close,volume,amount）"},
  {step:5,text:"步骤 5/7: 写入 factor/alpha101 — 新建 part 0000，包含 1 行（alpha001=0.3, alpha002=0.5）"},
  {step:6,text:"步骤 6/7: 写入 fieldset/ma — 新建 part 0000，包含 1 行（ma20=20.0）"},
  {step:7,text:"步骤 7/7: 两阶段提交 — 先写 <code>_tmp/transaction-42/</code> → 原子 move 到正式分区目录 → 提交完成"},
];
var updateNodes=[],updateCanvas;

function initUpdate(){
  updateCanvas=document.getElementById("update-canvas");
  updateNodes=[
    makeNode("group-index","🔑 index",8,8,65,28),
    makeNode("partition","month=2026-09",12,40,110,24),
    makeNode("partition","month=2026-10 (new)",12,70,120,24),
    makeNode("part-file","0000-0000000001.parquet",16,96,170,24),
    makeNode("group-factor","🧮 alpha101",35,8,85,28),
    makeNode("partition","month=2026-10 (new)",39,70,120,24),
    makeNode("part-file","0000-0000000001.parquet",43,96,170,24),
    makeNode("group-ma","📐 ma",62,8,65,28),
    makeNode("partition","month=2026-10 (new)",66,70,120,24),
    makeNode("part-file","0000-0000000001.parquet",70,96,170,24),
  ];
  renderNodes(updateCanvas,updateNodes);
  setStepText(updateCanvas,"点击 ▶ 播放 开始演示写入/更新流程");
}

function playUpdate(){
  resetCRUD("update");
  var stepEl=document.getElementById("update-step");
  stepEl.innerHTML="步骤 <b>1</b> / 7";
  [updateNodes[0],updateNodes[1]].forEach(function(n,i){showNode(n,i*100);});
  setStepText(updateCanvas,updateSteps[0].text);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>2</b> / 7";
    highlightNode(updateNodes[1]);
    setStepText(updateCanvas,updateSteps[1].text);
  },1500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>3</b> / 7";
    showNode(updateNodes[2],100);
    showNode(updateNodes[5],200);
    showNode(updateNodes[8],300);
    [updateNodes[2],updateNodes[5],updateNodes[8]].forEach(function(n){highlightNode(n);});
    setStepText(updateCanvas,updateSteps[2].text);
  },3000);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>4</b> / 7";
    showNode(updateNodes[3],100);
    writingNode(updateNodes[3]);
    drawArrow(updateCanvas,updateNodes[2],updateNodes[3]);
    setStepText(updateCanvas,updateSteps[3].text);
  },4500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>5</b> / 7";
    showNode(updateNodes[6],100);
    writingNode(updateNodes[6]);
    drawArrow(updateCanvas,updateNodes[5],updateNodes[6]);
    setStepText(updateCanvas,updateSteps[4].text);
  },6000);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>6</b> / 7";
    showNode(updateNodes[9],100);
    writingNode(updateNodes[9]);
    drawArrow(updateCanvas,updateNodes[8],updateNodes[9]);
    setStepText(updateCanvas,updateSteps[5].text);
  },7500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>7</b> / 7";
    [updateNodes[3],updateNodes[6],updateNodes[9]].forEach(function(n){
      if(n.el){n.el.classList.remove("writing");n.el.style.borderColor="var(--grn)";}
    });
    setStepText(updateCanvas,updateSteps[6].text);
  },9000);
}
