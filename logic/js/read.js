"use strict";
// READ animation: aligned_scan — 多列组并行扫描

var readSteps=[
  {step:1,text:"步骤 1/6: <code>SELECT date, symbol, close, alpha001, ma20 FROM aligned_scan('cnstk_ixday') WHERE date = '2026-09-01'</code>"},
  {step:2,text:"步骤 2/6: 解析查询 — 需要 index(close,date,symbol) + factor/alpha101(alpha001) + fieldset/ma(ma20) 三个列组"},
  {step:3,text:"步骤 3/6: 分区裁剪 — WHERE date='2026-09-01' → 只打开 month=2026-09 分区，跳过 month=2026-07"},
  {step:4,text:"步骤 4/6: 打开三个列组的 Parquet Reader，各自只读选中的列（Projection Pushdown）"},
  {step:5,text:"步骤 5/6: Row Group 裁剪 — 用 RG 统计信息跳过不包含目标日期的 Row Group"},
  {step:6,text:"步骤 6/6: DataChunk 组装 — 三个 Reader 的 Vector 直接填充同一行，不做 JOIN，缺分区 NULL 填充"},
];
var readNodes=[],readCanvas;

function initRead(){
  readCanvas=document.getElementById("read-canvas");
  readNodes=[
    makeNode("group-index","🔑 index",10,8,70,28),
    makeNode("partition","month=2026-07",13,42,110,24),
    makeNode("partition","month=2026-09",13,72,110,24),
    makeNode("part-file","0000-0000005998.parquet",16,98,170,24),
    makeNode("group-factor","🧮 alpha101",40,8,90,28),
    makeNode("partition","month=2026-07",43,42,110,24),
    makeNode("partition","month=2026-09",43,72,110,24),
    makeNode("part-file","0000-0000005998.parquet",46,98,170,24),
    makeNode("group-ma","📐 ma",70,8,70,28),
    makeNode("partition","month=2026-09",73,72,110,24),
    makeNode("part-file","0000-0000005998.parquet",76,98,170,24),
  ];
  renderNodes(readCanvas,readNodes);
  setStepText(readCanvas,"点击 ▶ 播放 开始演示查询全流程");
}

function playRead(){
  resetCRUD("read");
  var stepEl=document.getElementById("read-step");
  stepEl.innerHTML="步骤 <b>1</b> / 6";
  readNodes.forEach(function(n,i){showNode(n,i*80);});
  setStepText(readCanvas,readSteps[0].text);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>2</b> / 6";
    highlightNode(readNodes[0]);highlightNode(readNodes[4]);highlightNode(readNodes[8]);
    setStepText(readCanvas,readSteps[1].text);
  },1500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>3</b> / 6";
    [readNodes[1],readNodes[5]].forEach(function(n){if(n.el)n.el.style.opacity="0.25";});
    [readNodes[2],readNodes[6],readNodes[9]].forEach(function(n){highlightNode(n);});
    setStepText(readCanvas,readSteps[2].text);
  },3000);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>4</b> / 6";
    [readNodes[3],readNodes[7],readNodes[10]].forEach(function(n){highlightNode(n);});
    setStepText(readCanvas,readSteps[3].text);
  },4500);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>5</b> / 6";
    setStepText(readCanvas,readSteps[4].text);
  },6000);

  setTimeout(function(){
    stepEl.innerHTML="步骤 <b>6</b> / 6";
    setStepText(readCanvas,readSteps[5].text);
  },7500);
}
