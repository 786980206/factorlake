"use strict";
// Slide 0: original directory structure click interactions
(function(){
  var tableBox=document.querySelector(".table-box");
  if(!tableBox)return;

  document.querySelectorAll(".partition-header").forEach(function(h){
    h.addEventListener("click",function(e){
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
    var hasP=document.querySelector(".partition.selected"),hasPt=document.querySelector(".part.selected"),hasRG=document.querySelector(".rg.selected");
    tableBox.classList.remove("has-sel","has-sel-part","has-sel-rg");
    if(hasRG)tableBox.classList.add("has-sel","has-sel-part","has-sel-rg");
    else if(hasPt)tableBox.classList.add("has-sel","has-sel-part");
    else if(hasP)tableBox.classList.add("has-sel");
  }
})();
