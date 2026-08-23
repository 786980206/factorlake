"use strict";
// Slide navigation: click nav buttons to switch pages
(function(){
  document.querySelectorAll(".slide-nav .nav-btn").forEach(function(btn){
    btn.addEventListener("click",function(){
      var idx=btn.getAttribute("data-slide");
      document.querySelectorAll(".slide-nav .nav-btn").forEach(function(b){b.classList.remove("active")});
      btn.classList.add("active");
      document.querySelectorAll(".slide-page").forEach(function(p){p.classList.remove("active")});
      document.querySelector('.slide-page[data-page="'+idx+'"]').classList.add("active");
    });
  });
})();
