
var selectFastTrackOptions = {};

var fastTrackOptions = "<table id='malariaDrugs' cellspacing='0px' style='width:80%; left:10%; margin-left: 101px; font-size: 14pt;'>";
fastTrackOptions += "<tr>";
fastTrackOptions += "<th style='border-bottom: 1px solid black; padding:8px;'>&nbsp;</th>"
fastTrackOptions += "<th style='border: 0px solid black; padding:8px;'>&nbsp;</th>"
fastTrackOptions += "</tr>";

uncheckedImg = '/touchscreentoolkit/lib/images/unticked.jpg';
checkedImg = '/touchscreentoolkit/lib/images/ticked.jpg';
 
fastTrackOptions += "<tr id='' row_id = '1' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Age > 18 years and on ART > 1 year</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_1' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

fastTrackOptions += "<tr id='' row_id = '2' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Not On Second Line Treatment OR on IPT</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_2' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

fastTrackOptions += "<tr id='' row_id = '3' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Last VL < 1000, no VL Result pending, no VL taken at next visit</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_3' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

fastTrackOptions += "<tr id='' row_id = '4' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Not Pregnant? - no EID needed at next visit</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_4' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

fastTrackOptions += "<tr id='' row_id = '5' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Adherence on last 2 visits was good (check pill count and history)</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_5' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

fastTrackOptions += "<tr id='' row_id = '6' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Patient not suffering from major side effects, signs of TB or HIV associated disease</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_6' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

fastTrackOptions += "<tr id='' row_id = '7' onclick = 'highLightSelectedRow(this);' style='cursor: pointer;' >";
fastTrackOptions += "<td style='border-bottom: 1px solid black; padding:8px; text-align: left;'>Patient not not need hypertension or diabetes care on next visit</td>";
fastTrackOptions += "<td style='border-bottom: 0px solid black; text-align: center;'><img id='img_7' src='" + uncheckedImg + "'></img></td>";
fastTrackOptions += "</tr>";

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
    clinicVisitButton.className = 'clinicVisitButton';
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
    cancelButton.className = 'nextButton';
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
    fastTrackVisitButton.className = 'cancelButton';
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
    img = document.getElementById('img_' + rowID );
    img_src_array = img.getAttribute("src").split("/");
    src = img_src_array[img_src_array.length - 1];
    if (src == 'unchecked.png'){
        uncheckRows();
        img.src = checkedImg;
        obj.style.backgroundColor = 'lightBlue';
        selectedDrugID = rowID;
        selectMalariaDrug = antiMalariaDrugsHash[parseInt(rowID)];
        current_selected_drug = selectMalariaDrug["drug_name"];
        //hackGenericDrugs();
    }else{
        oldColor = obj.getAttribute('color');
        selectMalariaDrug = {}
        obj.style.backgroundColor = oldColor;
        img.src = uncheckedImg;
        //removeDrugFromGenerics();

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

function disableEnableFinishButton(){
    /*finishButton = document.getElementsByClassName("finishButton")[0];
    if (finishButton){
        if (Object.keys(selectMalariaDrug).length == 0){
            finishButton.style.backgroundColor = 'gray';
            finishButton.onclick = function(){

            }
        }else{
            finishButton.style.backgroundColor = 'green';
            finishButton.onclick = function(){
                hideLibPopup();
                notifier();
                if (notifierInterval){
                    clearInterval(notifierInterval);
                    notifierInterval = window.setInterval("hideNotifier();", 2000);
                }
            }
        }
    }*/

}

window.setInterval("disableEnableFinishButton()", 200);

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