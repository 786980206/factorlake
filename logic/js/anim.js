"use strict";
// Animation engine: utilities for highlighting directory elements on the table

function clearAllAnim(){
  document.querySelectorAll(".anim-hit,.anim-skip,.anim-new,.anim-writing,.anim-deleting,.anim-gone,.anim-active,.anim-proj,.anim-dim").forEach(function(el){
    el.classList.remove("anim-hit","anim-skip","anim-new","anim-writing","anim-deleting","anim-gone","anim-active","anim-proj","anim-dim");
  });
  // Reset inline opacity/filter on all elements (partitions, parts, rgs, groups, col-chips, fields)
  document.querySelectorAll(".partition,.part,.rg,.group,.col-chip,.field").forEach(function(el){
    el.style.opacity="";
    el.style.filter="";
  });
}

// Selectors
function selPartition(group,p){
  return '.group[data-g="'+group+'"] .partition[data-p="'+p+'"]';
}
function selPart(group,p,part){
  return '.group[data-g="'+group+'"] .partition[data-p="'+p+'"] .part[data-part="'+part+'"]';
}
function selRG(group,p,part,rg){
  return '.group[data-g="'+group+'"] .partition[data-p="'+p+'"] .part[data-part="'+part+'"] .rg[data-rg="'+rg+'"]';
}
function selField(colName){
  return '.col-axis .field[data-col="'+colName+'"]';
}

function addCls(selector,cls){
  document.querySelectorAll(selector).forEach(function(el){el.classList.add(cls);});
}
function rmCls(selector,cls){
  document.querySelectorAll(selector).forEach(function(el){el.classList.remove(cls);});
}

function setStep(num,text){
  var bar=document.getElementById("stepBar");
  document.getElementById("stepNum").textContent=num;
  document.getElementById("stepText").innerHTML=text;
  bar.classList.add("show");
}

function showStepBar(){document.getElementById("stepBar").classList.add("show");}
function hideStepBar(){document.getElementById("stepBar").classList.remove("show");}

// Expand a partition (add "selected" so its body shows)
function expandPartition(group,p){
  document.querySelectorAll(selPartition(group,p)).forEach(function(el){el.classList.add("selected");});
}
function expandPart(group,p,part){
  expandPartition(group,p);
  document.querySelectorAll(selPart(group,p,part)).forEach(function(el){el.classList.add("selected");});
}
