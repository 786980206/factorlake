"use strict";
// CREATE animation: aligned_create — 建表 / 扩展列组

var createSteps=[
  {step:1,text:"步骤 1/5: <code>aligned_create('cnstk_ixday','index','symbol VARCHAR,date DATE,close DOUBLE,volume BIGINT,amount DOUBLE')</code> — 创建逻辑表目录"},
  {step:2,text:"步骤 2/5: 创建 <code>index/</code> 列组目录，建立主键契约 (symbol VARCHAR + date DATE/TIMESTAMP)"},
  {step:3,text:"步骤 3/5: 创建默认分区 <code>month=1970-01</code>，写入 0 行占位 parquet — footer 携带完整 schema"},
  {step:4,text:"步骤 4/5: <code>aligned_create('cnstk_ixday','factor/alpha101','alpha001 DOUBLE,alpha002 DOUBLE,...')</code> — 扩展列组"},
  {step:5,text:"步骤 5/5: 为已有分区写 N 行全 NULL 占位 parquet — 满足分区对齐契约（N = index 分区行数 = 0）"},
];
var createNodes=[],createCanvas;

function initCreate(){
  createCanvas=document.getElementById("create-canvas");
  createNodes=[
    makeNode("group-index","🔑 index/",15,12,80,30),
    makeNode("partition","month=1970-01",20,50,120,25),
    makeNode("part-file","0000-0000000000.parquet<br>(0 rows)",25,85,180,35),
    makeNode("group-factor","🧮 factor/alpha101/",42,12,130,30),
    makeNode("partition","month=1970-01",47,50,120,25),
    makeNode("part-file","0000-0000000000.parquet<br>(NULL placeholder)",52,85,180,35),
  ];
  renderNodes(createCanvas,createNodes);
  setStepText(createCanvas,"点击 ▶ 播放 开始演示建表流程");
}

function playCreate(){
  resetCRUD("create");
  var stepEl=document.getElementById("create-step");
  stepEl.innerHTML="步骤 <b>1</b> / 5";
  showNode(createNodes[0],100);
  setStepText(createCanvas,createSteps[0].text);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>2</b> / 5";
    showNode(createNodes[1],200);
    setStepText(createCanvas,createSteps[1].text);
    drawArrow(createCanvas,createNodes[0],createNodes[1]);
  },1500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>3</b> / 5";
    showNode(createNodes[2],200);
    writingNode(createNodes[2]);
    setStepText(createCanvas,createSteps[2].text);
    drawArrow(createCanvas,createNodes[1],createNodes[2]);
  },3000);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>4</b> / 5";
    showNode(createNodes[3],200);
    showNode(createNodes[4],400);
    setStepText(createCanvas,createSteps[3].text);
    drawArrow(createCanvas,createNodes[0],createNodes[3]);
  },4500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>5</b> / 5";
    showNode(createNodes[5],200);
    writingNode(createNodes[5]);
    setStepText(createCanvas,createSteps[4].text);
    drawArrow(createCanvas,createNodes[4],createNodes[5]);
  },6000);

  setTimeout(function(){
    createNodes[2].el.classList.remove("writing");
    createNodes[5].el.classList.remove("writing");
    setStepText(createCanvas,"✅ 建表完成！表目录结构就绪，后续 INSERT 会创建真实分区并写入数据。");
  },7500);
}
