"use strict";
// Animation engine: shared utilities for CRUD slides

function makeNode(cls,text,x,y,w,h){
  return {cls:cls,text:text,x:x,y:y,w:w,h:h||28,el:null,state:"normal"};
}

function renderNodes(canvas,nodes){
  canvas.innerHTML="";
  var svgNS="http://www.w3.org/2000/svg";
  var svg=document.createElementNS(svgNS,"svg");
  svg.setAttribute("class","anim-svg");
  svg.setAttribute("width","100%");svg.setAttribute("height","100%");
  var defs=document.createElementNS(svgNS,"defs");
  var cid=canvas.id;
  defs.innerHTML='<marker id="arrowhead-'+cid+'" markerWidth="10" markerHeight="7" refX="8" refY="3.5" orient="auto">'+
    '<polygon points="0 0, 10 3.5, 0 7" fill="#0a7b83"/></marker>';
  svg.appendChild(defs);
  canvas.appendChild(svg);

  nodes.forEach(function(n,i){
    var el=document.createElement("div");
    el.className="anim-node "+n.cls+" hidden";
    el.style.left=n.x+"%";el.style.top=n.y+"%";
    el.style.width=n.w+"px";el.style.height=n.h+"px";
    el.innerHTML=n.text;
    el.style.transitionDelay=(i*0.05)+"s";
    canvas.appendChild(el);
    n.el=el;
  });
}

function showNode(n,delay){
  setTimeout(function(){if(n.el){n.el.classList.remove("hidden");n.el.classList.add("new");}},delay||0);
}
function highlightNode(n){if(n.el)n.el.classList.add("highlight");}
function writingNode(n){if(n.el)n.el.classList.add("writing");}
function updatingNode(n){if(n.el)n.el.classList.add("updating");}
function deletingNode(n){if(n.el)n.el.classList.add("deleting");}
function fadeOutNode(n){if(n.el)n.el.classList.add("fade-out");}
function unhighlightNode(n){if(n.el)n.el.classList.remove("highlight","writing","updating","deleting","fade-out");}

function drawArrow(canvas,from,to){
  var svg=canvas.querySelector("svg");
  if(!svg)return;
  var ns="http://www.w3.org/2000/svg";
  var line=document.createElementNS(ns,"path");
  var x1=from.x+from.w/2,y1=from.y+from.h/2;
  var x2=to.x+to.w/2,y2=to.y+to.h/2;
  line.setAttribute("d","M"+x1+"% "+y1+"% L"+x2+"% "+y2+"%");
  line.setAttribute("stroke","#0a7b83");
  line.setAttribute("stroke-width","2");
  line.setAttribute("fill","none");
  line.setAttribute("stroke-dasharray","4,3");
  line.setAttribute("marker-end","url(#arrowhead-"+canvas.id+")");
  line.setAttribute("opacity","0");
  line.style.transition="opacity .3s";
  svg.appendChild(line);
  setTimeout(function(){line.setAttribute("opacity","0.5");},50);
  return line;
}

function setStepText(canvas,html){
  var existing=canvas.querySelector(".anim-step-text");
  if(existing)existing.remove();
  var el=document.createElement("div");
  el.className="anim-step-text show";
  el.innerHTML=html;
  canvas.appendChild(el);
}
