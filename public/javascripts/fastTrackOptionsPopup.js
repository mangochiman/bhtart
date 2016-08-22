
var selectFastTrackOptions = {};
var selectedFastTrackConcepts = []

var fastTrackOptions = "<table id='malariaDrugs' cellspacing='0px' style='width:80%; left:10%; margin-left: 101px; font-size: 14pt;'>";
fastTrackOptions += "<tr>";
fastTrackOptions += "<th style='border-bottom: 1px solid black; padding:8px;'>&nbsp;</th>"
fastTrackOptions += "<th style='border: 0px solid black; padding:8px;'>&nbsp;</th>"
fastTrackOptions += "</tr>";

uncheckedImg = '/touchscreentoolkit/lib/images/unticked.jpg';
checkedImg = '/touchscreentoolkit/lib/images/ticked.jpg';

for (var pos in fastTrackAssesmentConcepts){
    concept_name = fastTrackAssesmentConcepts[pos]["concept_name"];
    concept_id = fastTrackAssesmentConcepts[pos]["concept_id"];
    
    fastTrackOptions += "<tr id='' row_id = '" + concept_id + "' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
    fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>" + concept_name + "</td>";
    fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_" + concept_id + "' src='" + uncheckedImg + "'></img></td>";
    fastTrackOptions += "</tr>";
}

fastTrackOptions += "</table>"

function fastTrackAssesmentPopup(){
    content = document.getElementById('content');
    popupDiv = document.createElement('div');
    popupDiv.className = 'popup-div';
    popupDiv.style.backgroundColor = '#F4F4F4';
    popupDiv.style.border = '2px solid #E0E0E0';
    popupDiv.style.borderRadius = '15px';
    popupDiv.style.height = '550px';
    popupDiv.style.top = '2%';
    popupDiv.style.left = '3%';
    popupDiv.style.marginTop = '-20px';
    popupDiv.style.marginLeft = '-20px';
    popupDiv.style.position = 'absolute';
    popupDiv.style.marginTop = '70px';
    popupDiv.style.width = '95%';
    popupDiv.style.zIndex = '991';
    content.appendChild(popupDiv);

    popupHeader = document.createElement('div');
    popupHeader.className = 'popup-header';
    popupHeader.innerHTML = 'Fast Track Assesment';
    popupHeader.style.borderBottom = '2px solid #7D9EC0';
    popupHeader.style.backgroundColor = '#FFFFFF';
    popupHeader.style.paddingTop = '5px';
    popupHeader.style.borderRadius = '15px 15px 0 0';
    popupHeader.style.fontSize = '16pt';
    popupHeader.style.fontWeight = 'bolder';


    popupDiv.appendChild(popupHeader);
    popupData = document.createElement('div');
    popupData.className = 'popup-data';
    popupData.innerHTML = fastTrackOptions;
    popupDiv.appendChild(popupData);
    popupFooter = document.createElement('div');
    popupFooter.className = 'popup-footer';
    popupFooter.style.position = 'absolute';
    popupFooter.style.marginBottom = '60px';

    clinicVisitButton = document.createElement('span');
    clinicVisitButton.className = 'clinicVisitButton FastTrackBtn';
    clinicVisitButton.innerHTML = 'Next Visit: Clinic Visit';
    clinicVisitButton.style.backgroundImage = 'none';
    clinicVisitButton.style.border = '1px solid transparent';
    clinicVisitButton.style.borderRadius = '4px';
    clinicVisitButton.style.cursor = 'pointer';
    clinicVisitButton.style.display = 'inline-block';
    clinicVisitButton.style.fontSize = '20px';
    clinicVisitButton.style.fontWeight = 'bolder';
    clinicVisitButton.style.lineHeight = '1.94857';
    clinicVisitButton.style.position = 'absolute';
    clinicVisitButton.style.bottom = '10px';
    clinicVisitButton.style.padding = '9px 54px';
    clinicVisitButton.style.textAlign = 'center';
    clinicVisitButton.style.verticalAlign = 'middle';
    clinicVisitButton.style.whiteSpace = 'nowrap';
    clinicVisitButton.style.backgroundColor = '#FF7F24';
    clinicVisitButton.style.color = 'black';
    
    clinicVisitButton.onclick = function(){
        hideLibPopup();
        notifier();
        setClinicVisit();
    }
    
    popupDiv.appendChild(clinicVisitButton);

    cancelButton = document.createElement('span');
    cancelButton.className = 'cancelButton FastTrackBtn';
    cancelButton.innerHTML = 'Cancel';
    cancelButton.style.backgroundImage = 'none';
    cancelButton.style.border = '1px solid transparent';
    cancelButton.style.borderRadius = '4px';
    cancelButton.style.cursor = 'pointer';
    cancelButton.style.display = 'inline-block';
    cancelButton.style.fontSize = '20px';
    cancelButton.style.fontWeight = 'bolder';
    cancelButton.style.lineHeight = '1.94857';
    cancelButton.style.position = 'absolute';
    cancelButton.style.bottom = '10px';
    cancelButton.style.padding = '9px 75px';
    cancelButton.style.textAlign = 'center';
    cancelButton.style.verticalAlign = 'middle';
    cancelButton.style.whiteSpace = 'nowrap';
    cancelButton.style.backgroundColor = '#DC143C';
    cancelButton.style.borderColor = '#6495ED';
    cancelButton.style.color = 'black';
    cancelButton.style.left = '22.6%';
    cancelButton.onclick = function(){
        cancelFastTrackPopup();
    }

    popupDiv.appendChild(cancelButton);

    fastTrackVisitButton = document.createElement('span');
    fastTrackVisitButton.className = 'fastTrackVisitButton FastTrackBtn';
    fastTrackVisitButton.innerHTML = 'Next Visit: Fast Track Visit';
    fastTrackVisitButton.style.backgroundImage = 'none';
    fastTrackVisitButton.style.border = '1px solid transparent';
    fastTrackVisitButton.style.borderRadius = '4px';
    fastTrackVisitButton.style.cursor = 'pointer';
    fastTrackVisitButton.style.display = 'inline-block';
    fastTrackVisitButton.style.fontSize = '20px';
    fastTrackVisitButton.style.fontWeight = 'bolder';
    fastTrackVisitButton.style.lineHeight = '1.94857';
    fastTrackVisitButton.style.position = 'absolute';
    fastTrackVisitButton.style.bottom = '10px';
    fastTrackVisitButton.style.right = '0px';
    fastTrackVisitButton.style.padding = '9px 26px';
    fastTrackVisitButton.style.textAlign = 'center';
    fastTrackVisitButton.style.verticalAlign = 'middle';
    fastTrackVisitButton.style.whiteSpace = 'nowrap';
    fastTrackVisitButton.style.backgroundColor = '#C1FFC1';
    fastTrackVisitButton.style.borderColor = '#00688B';
    fastTrackVisitButton.style.color = 'black';
    //fastTrackVisitButton.style.left = '81%';
    fastTrackVisitButton.onclick = function(){
        hideLibPopup();
        setFastTrackVisit();
    //selectMalariaDrug = {}; //Remove the selected drug
    //removeDrugFromGenerics();
    }

    popupDiv.appendChild(fastTrackVisitButton);

    popupDiv.appendChild(popupFooter);

    popupCover = document.createElement('div');
    popupCover.className = 'popup-cover';
    popupCover.style.position = 'absolute';
    popupCover.style.backgroundColor = 'black';
    popupCover.style.width = '100%';
    popupCover.style.height = '102%';
    popupCover.style.left = '0%';
    popupCover.style.top = '0%';
    popupCover.style.zIndex = '990';
    popupCover.style.opacity = '0.65';
    content.appendChild(popupCover);

//loadPreviousSelectedDrug(); //Preselect previously selected values
}

function highLightSelectedRow(obj){
    rowID = obj.getAttribute('row_id');
    concept_id = rowID;
    img = document.getElementById('img_' + rowID );
    img_src_array = img.getAttribute("src").split("/");
    src = img_src_array[img_src_array.length - 1];
    console.log(src)
    if (src == 'unticked.jpg'){
        img.src = checkedImg;
        obj.style.backgroundColor = 'lightBlue';
        selectedFastTrackConcepts.push(concept_id);
    }else{
        var index = selectedFastTrackConcepts.indexOf(concept_id);
        if (index > -1) {
            selectedFastTrackConcepts.splice(index, 1);
        }
        obj.style.backgroundColor = '';
        img.src = uncheckedImg;


    }

}

function uncheckRows(){
    selectMalariaDrug = {};
    current_selected_drug = null;
    malariaDrugsTable = document.getElementById('malariaDrugs');
    table_rows = malariaDrugsTable.getElementsByTagName('tr');
    for (var i=0; i<=table_rows.length - 1; i++){
        row = table_rows[i];
        if (row.hasAttribute('row_id')){
            rID = row.getAttribute('row_id');
            oldColor = row.getAttribute('color');
            row.style.backgroundColor = oldColor;
            mycheckedImg = document.getElementById('img_' + rID );
            mycheckedImg.src = uncheckedImg;
            d_name = row.getAttribute('drug_name');

            if (selectedGenerics[current_diagnosis]){
                if (selectedGenerics[current_diagnosis][d_name]){
                    delete selectedGenerics[current_diagnosis][d_name];
                }
            }
        }
    }
    
    if (selectedGenerics[current_diagnosis]){
        if (Object.keys(selectedGenerics[current_diagnosis]).length == 0){
            delete selectedGenerics[current_diagnosis] //it has no data
        }
    }
}

function disableEnableFastTrackVisitButton(){
    fastTrackVisitButton = document.getElementsByClassName("fastTrackVisitButton")[0];
    if (fastTrackVisitButton){
        if (selectedFastTrackConcepts.length < 7){
            fastTrackVisitButton.style.backgroundColor = '#dddddd';
            fastTrackVisitButton.onclick = function(){

            }
        }else{
            fastTrackVisitButton.style.backgroundColor = '#C1FFC1';
            fastTrackVisitButton.onclick = function(){
                hideLibPopup();
            }
        }
    }
}

window.setInterval("disableEnableFastTrackVisitButton()", 200);

function hideLibPopup(){
    popupCover = document.getElementsByClassName("popup-cover")[0];
    popupDiv = document.getElementsByClassName("popup-div")[0];
    if (popupCover) popupCover.parentNode.removeChild(popupCover);
    if (popupDiv) popupDiv.parentNode.removeChild(popupDiv);
}

function notifier(){
    content = document.getElementById('content');
    popupDiv = document.createElement('div');
    popupDiv.className = 'popup-div-notifier';
    popupDiv.style.backgroundColor = 'white';
    popupDiv.style.border = '2px solid #DDDD';
    popupDiv.style.borderRadius = '15px';
    popupDiv.style.height = '100px';
    popupDiv.style.top = '2%';
    popupDiv.style.left = '32%';
    popupDiv.style.marginTop = '-20px';
    popupDiv.style.position = 'absolute';
    popupDiv.style.marginTop = '158px';
    popupDiv.style.width = '600px';
    popupDiv.style.zIndex = '991';
    content.appendChild(popupDiv);

    popupHeader = document.createElement('div');
    popupHeader.className = 'popup-header-notifier';
    popupHeader.innerHTML = 'Notifications';
    popupHeader.style.borderBottom = '2px solid #7D9EC0';
    popupHeader.style.backgroundColor = '#FFFFFF';
    popupHeader.style.paddingTop = '5px';
    popupHeader.style.paddingLeft = '5px';
    popupHeader.style.borderRadius = '15px 15px 0 0';
    popupHeader.style.fontSize = '14pt';
    popupHeader.style.fontWeight = 'bolder';


    popupDiv.appendChild(popupHeader);
    popupData = document.createElement('div');
    popupData.className = 'popup-data-notifier';
    popupData.innerHTML = "Test Data Heidlshdlshdlshdlhsdlsdhl"
    popupData.style.fontSize = '18pt';
    popupData.style.fontWeight = 'bolder';
    popupData.style.textAlign = 'center';
    popupDiv.appendChild(popupData);
    popupFooter = document.createElement('div');
    popupFooter.className = 'popup-footer-notifier';
    popupFooter.style.position = 'absolute';
    popupFooter.style.marginBottom = '60px';

    popupCover = document.createElement('div');
    popupCover.className = 'popup-cover-notifier';
    popupCover.style.position = 'absolute';
    popupCover.style.backgroundColor = 'black';
    popupCover.style.width = '100%';
    popupCover.style.height = '102%';
    popupCover.style.left = '0%';
    popupCover.style.top = '0%';
    popupCover.style.zIndex = '990';
    popupCover.style.opacity = '0.65';
    content.appendChild(popupCover);
}

function hideNotifier(){
    popupCover = document.getElementsByClassName("popup-cover-notifier")[0];
    popupDiv = document.getElementsByClassName("popup-div-notifier")[0];
    if (popupCover) popupCover.parentNode.removeChild(popupCover);
    if (popupDiv) popupDiv.parentNode.removeChild(popupDiv);
}

var notifierInterval = window.setInterval("hideNotifier();", 2000);