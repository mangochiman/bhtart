var new_table;

function viewMoreSideEffects(){
    //popup-data
    moreButton = document.getElementsByClassName("more")[0];
    headerpopup = document.getElementById("header-popup");
    if (moreButton.innerHTML.match(/whole/i)){
        moreButton.innerHTML = 'Back to <br />current status';
        headerpopup.innerHTML = "Contraindications / Side effects (whole history)<br />"
        moreSideEffects()
    }else{
        moreButton.innerHTML = 'View whole <br />side effect history';
        headerpopup.innerHTML = "Contraindications / Side effects for selected regimen <br />"
        lessSideEffects();
    }
}

function moreSideEffects(){
    new_table = "<table cellspacing='0px'>";
    new_table += "<tr class='contraindications_row'>";
      new_table += "<th>&nbsp;</th>";
      new_table += "<th>Contraindications / Side effect</th>";
      new_table += "<th>Condition</th>";
    new_table += "</tr>";

    sorted_dates = sortDates(Object.keys(history_of_side_effects));
    for (var i=0; i<=sorted_dates.length - 1; i++){
        date = sorted_dates[i];
        new_table += "<tr>";
        new_table += "<td>" + date + "</td>";

        if (date_of_first_hiv_clinic_enc.getTime() == new Date(date).getTime()){
            new_table += "<td>Contraindications</td>";
        }else{
            new_table += "<td>Side Effects</td>";
        }
        new_table += "<td>" + history_of_side_effects[date].join('<br />') + "</td>";
        new_table += "</tr>";
    }

    /*for (var date in history_of_side_effects){
        new_table += "<tr>";
        new_table += "<td>" + date + "</td>";

        if (date_of_first_hiv_clinic_enc.getTime() == new Date(date).getTime()){
            new_table += "<td>Contraindications</td>";
        }else{
            new_table += "<td>Side Effects</td>";
        }
        new_table += "<td>" + history_of_side_effects[date].join('<br />') + "</td>";
        new_table += "</tr>";
    }*/

    new_table += "</table>";
    document.getElementsByClassName("popup-data")[0].innerHTML = new_table;
}

function lessSideEffects(){
/*  
    new_table = "<table cellspacing='0px'>";
    new_table += "<tr>";
    new_table += "<td>" + today + "</td>";
    new_table += "<td>" + flattedContraindications.join('<br />') + "</td>";
    new_table += "</tr>";
    new_table += "</table>";
*/
    type = 'Side Effect';
    new_table = "<table cellspacing='0px'>";
    new_table += "<tr class='contraindications_row'>";
      new_table += "<th>&nbsp;</th>";
      new_table += "<th>Contraindications / Side effect</th>";
      new_table += "<th>Condition</th>";
    new_table += "</tr>";

    for(var i = 0; i < flattedContraindications.length; i++) {
        if (date_of_first_hiv_clinic_enc.getTime() == new Date(today).getTime()){
            type = 'Contraindication';
        }
      new_table += "<tr>";
        new_table += "<td>" + today + "</td>";
        new_table += "<td>" + type + "</td>";
        new_table += "<td>" + flattedContraindications[i] + "</td>";
      new_table += "</tr>";
    }
    new_table += "</table>";
    document.getElementsByClassName("popup-data")[0].innerHTML = new_table;
}

function contraindicators(){
    regimen_concept_id = document.getElementById('regimen_concept_id')
    try {
            selectedRegimenIndex = regimen_concept_id.options[regimen_concept_id.selectedIndex].text.split(" ")[1];
        }
    catch(err) {
        __$('touchscreenInput' + tstCurrentPage).removeAttribute("optional", "true");
        gotoNextPage();//This section is just a hack to alert the error message when nothing is selected.
        return false;
    }


    selectedRegimenContraindications = adverse_events[selectedRegimenIndex]["contraindications"];
    regimenAltOne = adverse_events[selectedRegimenIndex]["alt1"];
    regimenAltTwo = adverse_events[selectedRegimenIndex]["alt2"];

    flattedContraindications = [];
    matchedSideEffects = false;

    for (var i=0; i<=selectedRegimenContraindications.length - 1; i++){
        //flattedContraindications.push(selectedRegimenContraindications[i][0]);
        for (var z=0; z<=sideEffectsAnswers.length - 1; z++){
            if (selectedRegimenContraindications[i][0].toUpperCase().match(sideEffectsAnswers[z].toUpperCase())){
                matchedSideEffects = true;
                flattedContraindications.push(sideEffectsAnswers[z]);
                //break;
            }
        }
    }

    new_table = "<table cellspacing='0px'>";
    new_table += "<tr class='contraindications_row'>";
      new_table += "<th>&nbsp;</th>";
      new_table += "<th>Contraindications / Side effect</th>";
      new_table += "<th>Condition</th>";
    new_table += "</tr>";
    type = 'Side Effect';
    for(var i = 0; i < flattedContraindications.length; i++) {
        if (date_of_first_hiv_clinic_enc.getTime() == new Date(today).getTime()){
            type = 'Contraindication';
        }
      new_table += "<tr>";
        new_table += "<td>" + today + "</td>";

        new_table += "<td>" + type + "</td>";
        new_table += "<td>" + flattedContraindications[i] + "</td>";
      new_table += "</tr>";
    }
    new_table += "</table>";

    if (matchedSideEffects){
        alt_one_drugs = [];
        alt_one_drugs_data = "";
        alt_two_drugs_data = "";
        alt_two_drugs = [];

        for (var j=0; j<=sideEffectsAnswers.length - 1; j++){
            for (var k=0; k<=regimenAltOne.length - 1; k++){
                if (sideEffectsAnswers[j].toUpperCase().match(regimenAltOne[k][0].toUpperCase())){
                    alt_one_drugs.push([sideEffectsAnswers[j], regimenAltOne[k][1]])
                    alt_one_drugs_data += sideEffectsAnswers[j] + ": Regimen " + regimenAltOne[k][1] + " ";
                }
            }
            for (var y=0; y<=regimenAltTwo.length - 1; y++){
                if (sideEffectsAnswers[j].toUpperCase().match(regimenAltTwo[y][0].toUpperCase())){
                    alt_two_drugs.push([sideEffectsAnswers[j], regimenAltTwo[y][1]]);
                    alt_two_drugs_data += sideEffectsAnswers[j] + ": Regimen " + regimenAltTwo[y][1] + " ";
                }
            }
        }

        data = "<table cellspacing='0px' style='width:40%; left:10%; margin-left: 101px; font-size: 14pt;'>";
        data += "<tr>";
        data += "<td style='border-bottom: 1px solid black; padding:8px;'>&nbsp;</td>"
        data += "<td style='border: 0px solid black; padding:8px;'>&nbsp;</td>"
        data += "</tr>";
        data += "</table>";

        sideEffectsData = "<b><span style='color: green;'>Side Effects</span>: <i>" + sideEffectsAnswers.join(", ") + "</i></b><br /><br />";
        sideEffectsData += "<b><span style='color: green;'>Regimen " + selectedRegimenIndex + " Contraindications</span>: <i>" + flattedContraindications.join(", ") + "</i></b><br /><br />";
        if (alt_one_drugs.length > 0) sideEffectsData += "<b><span style='color: green;'>Alternative Regimen 1</span>: <i> " + alt_one_drugs_data + "</i></b><br /><br />";
        if (alt_two_drugs.length > 0) sideEffectsData += "<b><span style='color: green;'>Alternative Regimen 2</span>: <i> " + alt_two_drugs_data + "</i></b><br /><br />";

        content = document.getElementById('content');
        popupDiv = document.createElement('div');
        popupDiv.className = 'popup-div';
        popupDiv.style.backgroundColor = '#F4F4F4';
        popupDiv.style.border = '2px solid #E0E0E0';
        popupDiv.style.borderRadius = '15px';
        popupDiv.style.height = '503px';
        popupDiv.style.top = '2%';
        popupDiv.style.left = '23%';
        popupDiv.style.marginTop = '-20px';
        popupDiv.style.marginLeft = '-20px';
        popupDiv.style.position = 'absolute';
        popupDiv.style.marginTop = '29px';
        popupDiv.style.width = '56%';
        popupDiv.style.zIndex = '991';
        content.appendChild(popupDiv);

        popupHeader = document.createElement('div');
        popupHeader.className = 'popup-header';
        popupHeader.innerHTML = '<span id="header-popup">Contraindications / Side effects for selected regimen <br /></span>';
        popupHeader.style.borderBottom = '2px solid #7D9EC0';
        popupHeader.style.backgroundColor = '#FFFFFF';
        popupHeader.style.paddingTop = '5px';
        popupHeader.style.borderRadius = '15px 15px 0 0';
        popupHeader.style.fontSize = '16pt';
        popupHeader.style.textAlign = 'center';
        popupHeader.style.fontWeight = 'bolder';


        popupDiv.appendChild(popupHeader);
        popupData = document.createElement('div');
        popupData.className = 'popup-data';
        popupData.innerHTML = new_table;//sideEffectsData;
        popupData.style.overflow = "auto";
        popupData.style.height = "69%";
        popupData.style.fontSize = "16pt";
        popupData.style.marginTop = '20px';
        popupData.style.marginLeft = '20px';
        popupDiv.appendChild(popupData);
        popupFooter = document.createElement('div');
        popupFooter.className = 'popup-footer';
        popupFooter.style.position = 'absolute';
        popupFooter.style.marginBottom = '60px';

        cancelButton = document.createElement('span');
        cancelButton.className = 'clinicVisitButton FastTrackBtn';
        cancelButton.innerHTML = 'Select Another <br />Regimen';
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
        cancelButton.style.padding = '3px 40px';
        cancelButton.style.left = '10px';
        cancelButton.style.textAlign = 'center';
        cancelButton.style.verticalAlign = 'middle';
        cancelButton.style.whiteSpace = 'nowrap';
        cancelButton.style.backgroundColor = '#00B2EE';
        cancelButton.style.color = 'white';
    
        cancelButton.onclick = function(){
            hideLibPopup();
        }
    
        popupDiv.appendChild(cancelButton);

        viewMoreButton = document.createElement('span');
        viewMoreButton.className = 'clinicVisitButton FastTrackBtn more';
        viewMoreButton.innerHTML = 'View whole <br />side effect history';
        viewMoreButton.style.backgroundImage = 'none';
        viewMoreButton.style.border = '1px solid transparent';
        viewMoreButton.style.borderRadius = '4px';
        viewMoreButton.style.cursor = 'pointer';
        viewMoreButton.style.display = 'inline-block';
        viewMoreButton.style.fontSize = '20px';
        viewMoreButton.style.fontWeight = 'bolder';
        viewMoreButton.style.lineHeight = '1.94857';
        viewMoreButton.style.position = 'absolute';
        viewMoreButton.style.bottom = '10px';
        viewMoreButton.style.padding = '3px 64px';
        viewMoreButton.style.left = '33%';
        viewMoreButton.style.textAlign = 'center';
        viewMoreButton.style.verticalAlign = 'middle';
        viewMoreButton.style.whiteSpace = 'nowrap';
        viewMoreButton.style.backgroundColor = '#4F94CD';
        viewMoreButton.style.color = 'white';

        viewMoreButton.onclick = function(){
            viewMoreSideEffects();
        }

        popupDiv.appendChild(viewMoreButton);

        nextButton = document.createElement('span');
        nextButton.className = 'fastTrackVisitButton FastTrackBtn';
        nextButton.innerHTML = 'Continue With The <br />Selected Regimen';
        nextButton.style.backgroundImage = 'none';
        nextButton.style.border = '1px solid transparent';
        nextButton.style.borderRadius = '4px';
        nextButton.style.cursor = 'pointer';
        nextButton.style.display = 'inline-block';
        nextButton.style.fontSize = '20px';
        nextButton.style.fontWeight = 'bolder';
        nextButton.style.lineHeight = '1.94857';
        nextButton.style.position = 'absolute';
        nextButton.style.bottom = '10px';
        nextButton.style.padding = '3px 40px';
        nextButton.style.right = '10px';
        nextButton.style.textAlign = 'center';
        nextButton.style.verticalAlign = 'middle';
        nextButton.style.whiteSpace = 'nowrap';
        nextButton.style.backgroundColor = '#228B22';
        nextButton.style.borderColor = '#00688B';
        nextButton.style.color = 'white';
        nextButton.onclick = function(){
            hideLibPopup();
            gotoNextPage();
        }

        popupDiv.appendChild(nextButton);

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
    }
    else{
        resetRegimen();
        gotoNextPage();
    }
}

function hideLibPopup(){
    popupCover = document.getElementsByClassName("popup-cover")[0];
    popupDiv = document.getElementsByClassName("popup-div")[0];
    if (popupCover) popupCover.parentNode.removeChild(popupCover);
    if (popupDiv) popupDiv.parentNode.removeChild(popupDiv);
}

function cancelRegimenSideEffectsPopup(){
    hideLibPopup();
}

function sortDates(array) {
    return array.sort(function(a, b) {
      var x = new Date(a).getTime(); var y = new Date(b).getTime();
        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
    });
}
