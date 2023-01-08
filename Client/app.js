function getBathValue() {
    var uiBathrooms = document.getElementsByName("uiBathrooms");
    for(var i in uiBathrooms) {
      if(uiBathrooms[i].checked) {
          return parseInt(i)+1;
      }
    }
    return -1; // Invalid Value
  }
  
  function getBHKValue() {
    var uiBHK = document.getElementsByName("uiBHK");
    for(var i in uiBHK) {
      if(uiBHK[i].checked) {
          return parseInt(i)+1;
      }
    }
    return -1; // Invalid Value
  }
  
  function onClickedEstimatePrice() {
    console.log("Estimate price button clicked");
    var total_sqft = document.getElementById("uiSqft");
    var bhk = getBHKValue();
    var bathrooms = getBathValue();
    var loc = document.getElementById("uiLocations");
    var ptype = document.getElementById("uiProperty");
    var atype = document.getElementById("uiAttachment");
    var estPrice = document.getElementById("uiEstimatedPrice");
  
    var url = "http://127.0.0.1:5000/predict_home_price"; //Use this if you are NOT using nginx which is first 7 tutorials
    // var url = "/api/predict_home_price"; // Use this if  you are using nginx. i.e tutorial 8 and onwards
  
    $.post(url, {
      sqft: parseFloat(total_sqft.value),
      bed: bhk,
      bath: bathrooms,
      location: loc.value,
      property_type: ptype.value,
      attachement: atype.value
  },function(data, status) {
      console.log(data.estimated_price);
      estPrice.innerHTML = "<h2> $" + data.estimated_price.toString() + " CAD</h2>";
      console.log(status);
  });
}
  