<template>
<div>
    <div class = "centerScreenText hideCenterText">
        <a onclick="window.open('/app/dashboard','_blank')"> <i class="fas fa-align-justify NewProccessIcon hoverexpand clickable"  style = "height: 40px; width: 40px"></i> </a>
        <a class = "NewProcessHover" onclick="window.open('/app/dashboard','_blank')" style = "display: block;"> Open a New Process </a>
        <div @click = "cancelCall" class = "icon hoverexpand clickable" style = "background-color: red"> <i class="fas fa-ban" style = "color: white;"></i></div>
        <a @click = "cancelCall" class = "CancelProcessHover" style = "display: block; color: red"> Cancel Process </a>
    </div>
    <div class="columns mt-0 pt-0 mx-2 notclickable is-mobile is-centered" style = "height: 70px; opacity: .8">
        <div class="column p-0 has-text-centered is-mobile" v-bind:class="{ 'is-4': columnsClass, 'is-8': !columnsClass }">
            <div class = "ButtonsDiv OptionsBorderRadius">
                <a class = "mx-3" @click="ExecutablePop"> <i class="fas fa-align-justify BDMIcon hoverexpand"  style = "height: 40px; width: 40px"></i> </a>
                <a class = "mx-3" href = "/support"> <i class="fas fa-align-justify BDMIconTwo hoverexpand"  style = "height: 40px; width: 40px"></i> </a>
            <div>
            </div>
            </div>
        </div>
    </div>
		<div class="columns mt-0 mb-2">
		<div class="column has-text-centered pb-0 pt-0">
        <section id="hero" class = "background-transparent pt-0">
          <div class="container">
            <div class="columns is-vcentered is-centered">
              <div class="column is-12 content-box hero-content p-0">
                <h1>  <b>SQL Merges Module</b></h1>
                <p class = "has-text-centered">Merge Differing Files with Complexity, Efficienty, and Ease</p>
              <div>
                <i class = "fas fa-align-justify DemoIcon" style = "transform: scale(1); height: 50px; width: 50px"/>
                <i class = "fas fa-align-justify HelpingIcon clickable hoverexpand ml-4" style = "height: 50px; width: 50px" @click = "showAlert"/>
              </div>
              </div>
            </div>
          </div>
        </section>			
      </div>
        </div>
    <hr class = "my-0 mx-5" style = "height: 3px">
    <div class="columns mt-5">
    <div class="column">
        <div v-on:click="appendJoinType" class = "has-shadow button is-rounded is-white hoverexpandSmall slimbutton is-fullwidth p-0" style = "height: 90px">
                <div class="column is-1 is-pulled-right p-0 pl-2">
                        <div class = "icon" style = "background-color: lightgreen"> <i class="fas fa-equals has-text-white text-break" aria-hidden="true"></i> </div>
                    </div> 
                <div class="column has-text-centered is-pulled-left p-0">
                        <div class = "has-text-weight-bold has-text-dark" style = "font-size: 20px">Join Type</div>
                        <div class = "has-text-grey-light">Join Type Block</div>
                </div>
                <div class="column is-1 has-text-centered is-pulled-left p-0 pr-2">
                    <i class="fas fa-dot-circle" style = "color: lightgreen;"></i>
                </div>    
                </div>
        </div>
    <div class="column">
        <div v-on:click="appendJoinData" class = "has-shadow button is-rounded is-white hoverexpandSmall slimbutton is-fullwidth p-0" style = "height: 90px">
                <div class="column is-1 is-pulled-right p-0 pl-2">
                        <div class = "icon" style = "background-color: lightpink"> <i class="fas fa-columns has-text-white text-break" aria-hidden="true"></i> </div>
                    </div> 
                <div class="column has-text-centered is-pulled-left p-0">
                        <div class = "has-text-weight-bold has-text-dark" style = "font-size: 20px">Join Data</div>
                        <div class = "has-text-grey-light">Join Data UI Block</div>
                </div>
                <div class="column is-1 has-text-centered is-pulled-left p-0 pr-2">
                    <i class="fas fa-dot-circle has-text-pink"></i>
                </div>    
                </div>
        </div>
    </div>

        <div class="columns is-desktop">
        <div class="column agColumn mt-4 pt-0 px-2">
            <!-- Setting allows fro Full Sidebar Rendering without having the Table render 1200 for no reason -->
            <div class = "card px-1 pt-3 pb-0 OptionsBorderRadius">
                <header class="slightround mb-3 customHeader" style = "box-shadow: none !important;">
                <div class="columns is-vcentered pb-2">
                    <div class="column is-6 has-text-centered">
                        <h2 class="h5 text-uppercase mb-0">Live Information of Data Upload - Select File</h2>
                    </div>
                    <div class="column is-6 px-0 has-text-centered" style = "outline: none !important;">
                        <InsertUpload :MutlipleSelect="MutlipleSelect" :resetData="resetData" :SetExecutionData="SetExecutionData"/>
                    </div>
                </div>
                <div class="columns">
                    <div class="column is-12 has-text-centered px-3">
                        <b-dropdown class = "mt-0"
                            :paddingless="true" :scrollable="true" :mobile-modal="false" max-height="135px" v-model="CurrentBaseTableSelect" aria-role="list" expanded append-to-body>
                            <button @click="AdjustWidth" class="button dark-success-background is-rounded has-text-white is-fullwidth dropdownPull" type="button" slot="trigger">
                                <span class = "elipsesText text-break widthUp">{{CurrentBaseTableSelect}}</span>
                                <b-icon icon="menu-down widthUpicon" style = "z-index: 1"></b-icon>
                            </button>
                            <b-dropdown-item class = "pt-0 is-fullwidth searchParent"
                                v-for="(FileObjectList, index) in FileObjectList"
                                :key="index"
                                :value="FileObjectList" aria-role="listitem">
                                <div class="media">
                                    <div class="media-content elipsesText">
                                        {{FileObjectList}}
                                    </div>
                                </div>
                            </b-dropdown-item>
                        </b-dropdown>
                        <small> Select Base Table </small>        
                    </div>
                </div>
                <hr class = 'mt-0 mx-5 mb-3' v-bind:class="{nodisplay: noDisplay}">
                <div class="columns is-vcentered is-centered" v-bind:class="{nodisplay: noDisplay}">
                    <div class="column is-2 has-text-centered p-0">
                                <a @click="AgGridReset"> <i class="fa-lg fas fa-power-off has-text-danger text-break" aria-hidden="true"></i> </a>
                                <div> <small>Reset Data</small> </div>
                    </div>
                </div>
                </header>
                <!-- Card takes on the Height of the Parent by setting an Actual Minimum Height for the Column ((Minimum is Important)), now just adjust the Height of the Grid to be Responsive based on Height of Component -->
                <AgGridAnalysis :SuccessHasTriggered="SuccessHasTriggered" :Module="Module" @displayOn="displayOn" @getExecutionData="getExecutionData" :ComponentDataObject="ComponentDataObject" :getExecutionDataTrigger="getExecutionDataTrigger" :MutlipleSelect="MutlipleSelect" :resetData="resetData" :addColumnTrigger="addColumnTrigger" :removeColumnTrigger="removeColumnTrigger" :copyColumnTrigger="copyColumnTrigger" @getFiles="getFiles" />
                <div class="columns is-vcentered is-centered" v-if="SuccessHasTriggered === false">
                    <div class="column is-10 has-text-centered p-0 pb-2">
                                <p class = "has-text-danger">Table Contents Load Only on Execution</p>
                    </div>
                </div>
                </div>
        </div>
        <!--
            Set the Min-Height on what stacks on the Bottom
            To adjust columns for Desktop and Up, add is-desktop and is-5-desktop or is-7-desktop
        -->
    
        <div class="column p-0 is-1-desktop" style = "width: 3% !important;"> </div>

        <div class="column p-0 pt-0 mt-4 px-2 customMargin is-5-desktop">
            <div class="columns has-shadow is-vcentered p-0 is-fullwidth mb-0 OptionsBorderRadius button is-primary notclickable HeaderModule" style = "height: 75px;">
                <div class="column is-7 p-0 m-0 HeaderModule is-vcentered OptionsBorderRadius" style = "height: 75px;">
                    <span class="OptionsBorderRadius HeaderModule title is-4 button is-primary notclickable p-0 m-0 text-break has-text-dark" style = "height: 75px; opacity: 1 !important;">Execution/Go</span>
                </div>
                <div class="column is-5 p-0 m-0 HeaderModule veryrounded is-centered mr-2" style = "height: 50px; opacity: 1 !important;">
                    <a @click="clearAll" class = "mx-2"> <i class="fas fa-align-justify CardIconCancel hoverexpand"  style = "height: 40px; width: 40px; opacity: 1 !important;"></i></a>
                    <a @click="injectData" class = "mx-2"> <i class="fas fa-align-justify CardIconGo hoverexpand"  style = "height: 40px; width: 40px; opacity: 1 !important;"></i> </a>
                </div>

            </div>
        <hr class = "mx-5 mb-2">
            <div class="columns is-vcentered is-centered" v-if="FileObjectList.length < 2">
                <div class="column is-10 has-text-centered p-0 pb-2">
                            <p class = "has-text-danger">Files Only Appear for Selection On 2+ Files Uploaded</p>
                </div>
            </div>
        <div class="columns mt-0 mb-0">
            <div class="column is-12 has-text-centered p-0 pb-1">
                <div v-on:click="appendFirst" class = "icon smallIcon clickable hoverexpand" style = "background-color: lightgreen"> <i class="fas fa-equals has-text-white text-break" aria-hidden="true"></i> </div>
            </div>
        </div>
        <SlideUpDownGroup>
        <component
            :is="block.Component"
            v-for="(block,index) in ComponentDataObject"
            @updateFileSelection="updateFileSelection" @removeBlock="removeBlock" @addComponent="addComponent" @triggerData="triggerData" :ComponentDataObject="ComponentDataObject" :ModuleErrors="block.ModuleErrors" :TriggerModuleError='block.TriggerModuleError' :FileInformation="FileObjectList" :key="block.identifier" :SubBlock="block" :TriggerCheck="TriggerCheck" :identifier="block.identifier" :blockIndex="index" :triggertotalObject="triggertotalObject" :totalObject="ComponentDataObject" :CurrentBaseTableSelect="CurrentBaseTableSelect"
        />
        </SlideUpDownGroup>
        </div>
    </div>
    <ExecutionItems 
    @loadfromSavesEmit = "loadfromSavesEmit" @ExecutionDataName = "ExecutionDataName" @ExecutablePopEmit = "ExecutablePopEmit" @addColumn = "addColumn" @removeColumn = "removeColumn" @copyColumn = "copyColumn" :Module="Module" :MostRecentExecutionData="MostRecentExecutionData" :EnableOptions = "EnableOptions" :isImageModalActiveProp = "ToggleExecutables" />
</div>
</template>

<script>
console.log("Warning: If youa removing indexes not 1 by 1, discover what to remove by Identifier, not Index, because Index Chagnes")
console.log("See Previous Saved Data Removal in Insert Upload")
import SQLMergesType from '@/_common/components/AnalyticsComponents/SQLMerges/SQLMergesType.vue';
import SQLMergesData from '@/_common/components/AnalyticsComponents/SQLMerges/SQLMergesData.vue';
import AgGridAnalysis from '@/_common/components/AnalyticsComponents/AgGridBase.vue';
import InsertUpload from '@/_common/components/AnalyticsComponents/InsertUpload.vue';
import SlideUpDownGroup from '@/_common/components/transitions/SlideUpDownGroup.vue'
import ExecutionItems from '@/_common/components/AnalyticsComponents/ExecutionItems.vue'
import cloneDeep from 'lodash.clonedeep'
import axios from 'axios';
axios.defaults.xsrfHeaderName = "X-CSRFTOKEN"
axios.defaults.xsrfCookieName = "csrftoken"
let cancelTokenSource = axios.CancelToken.source();
console.log("Cancel Token", cancelTokenSource)

export default {
    name: 'App',
    data() {
        return {
            Module: 'SQLMerges',
            MutlipleSelect: true, // Used to Change Upload Component and Executables Prompt Functionality
            EnableOptions: true, // Used to Trigger if Certain Executable Criteria should be Avaliable
            ToggleExecutables: false, // Used to Toggle the Executables Prompt
            ComponentDataObject: [], // Component Data Object
            TriggerCheck: 'No Check', // Used to Trigger Flow of Inject Data to form Execute List
            IdentifierGen: 0, // Identifier Generation so Components do not use Index as key
            columnsClass: true, // Column Adjustment for Smaller/Larger Screen for Particular Elements only
            resetData: false, // Reset AgGrid Data
            addColumnTrigger: false, // Emit Trigger for Adding a Column
            removeColumnTrigger: false, // Emit Trigger for Removing a Column
            copyColumnTrigger: false, // Emit Trigger for Copying a Column
            ExecuteList: [], // Current Execute List, whether Loaded or Formed
            ExecutionData: null, // Current Sent Execution Data,
            ExecutionDataSaveName: '',
            ExecutionCounter: 0,
            MostRecentExecutionData: [],
            getExecutionDataTrigger: false, // Emit Trigger for Sending Execution Data,
            SetExecutionData: false,
            noDisplay: true,
            //
            FileObjectList: [],
            SuccessHasTriggered: false,
            CurrentBaseTableSelect: "Select a Base Table",
            triggertotalObject: false,
            triggerFileUpdates: false
        }
    },
    components: {
        AgGridAnalysis,
        SQLMergesType,
        SQLMergesData,
        SlideUpDownGroup,
        ExecutionItems,
        InsertUpload
    },
    watch: {
        // Used to Change Column Class Width on Resizes
        windowWidth: function (val) {
            if (val <= 769) {
                this.columnsClass = false
            } else {
                this.columnsClass = true
            }
            if (val <= 769) {
                this.columnsClass = false
            } else {
                this.columnsClass = true
            }
        },
        '$route' () {
            // Used to Change Column Class Width on Route Changes incase of Cached Component
            if (document.body.clientWidth <= 769) {
                this.columnsClass = false
            } else {
                this.columnsClass = true
            }
        },
    },
    methods: {
        AdjustWidth: function (event) {
            setTimeout(() => { 
                const CurrentItems = document.getElementsByClassName("dropdown-menu-animation is-expanded dropdown")
                let TargetWidth = "To Set"
                if (event.target.classList.contains("widthUp")) {
                    TargetWidth = event.target.parentElement.offsetWidth + 'px'
                } else if (event.target.classList.contains("widthUpicon") || event.target.classList.contains("icon")) {
                    TargetWidth = event.target.parentElement.parentElement.offsetWidth + 'px'
                } else {
                    TargetWidth = event.target.offsetWidth + 'px'
                }
                for (let i = 0; i < CurrentItems.length; i++) {
                    try {
                        if (CurrentItems[i].getElementsByClassName("dropdown-menu")[0].style.display !== "none") {
                            CurrentItems[i].getElementsByClassName("dropdown-menu")[0].style.minWidth = TargetWidth
                        }
                    } catch {
                        //
                    }
                }
            }, 10)
        },
        displayOn: function () {
            // Used as an Emit to Add a Column (NCN)
            this.noDisplay = false
        },
        updateFileSelection: function () {
            this.triggertotalObject = true
            this.$nextTick(() => {
                setTimeout(() => { this.triggertotalObject = false }, 100)
            })
        },
        addColumn: function (val) {
            // Used as an Emit to Add a Column (NCN)
            this.addColumnTrigger = val
        },
        removeColumn: function (val) {
            // Used as an emit to Remove a Column (NCN)
            this.removeColumnTrigger = val
        },
        copyColumn: function (val) {
            // Used an an emit to Copy a Column (NCN)
            this.copyColumnTrigger = val
        },
        getExecutionData: function (val) {
            // Used to Gather the Current Execution Data
            console.log('Execution Data', val)
            this.ExecutionData = val
        },
        ExecutablePop: function () {
            // Used to toggle the Executeables Component (NCN)
            this.ToggleExecutables = true
        },
        ExecutablePopEmit: function (val) {
            // Used as an Emit to toggle the Executables Component (NCN)
            console.log(val)
            this.ToggleExecutables = val.Response
        },
        getFiles: function (val) {
            // Used as an Emit to gather any Column Changes (NCN)
            console.log(val)
            const BaseTables = []
            for (let i = 0; i < this.$store.state.ToggledDataList.length; i++) {
                const FileName = this.$store.state.ToggledDataList[i]['UniqueFileName']
                BaseTables.push(FileName)
            }
            this.FileObjectList = BaseTables
            if (!this.FileObjectList.includes(this.CurrentBaseTableSelect)) {
                this.CurrentBaseTableSelect = ""
            }
        },
        AgGridReset: function() {
            // Used as an Emit to Reset the AgGrid data (NCN)
            this.resetData = true
            this.$nextTick(() => {
                setTimeout(() => { this.resetData = false }, 100)
            })
        },
        ExecutionDataName: function (val) {
            this.ExecutionDataSaveName = val
        },
        loadfromSavesEmit: function (val) {
            console.log("Load Saves from Emit", val)
            if (val.length === 0) {
                return false
            }
            // Used an an Emit from the Executeables Dialouge to Load in Previously Saved Module (CN)
            const SavesExecuteList = val
            console.log(SavesExecuteList)
            this.clearAll() // Remount
            // Loop Through Each Execution Loop, add a component each time X stop hits
            // For example, if you hit an identifier, then you trigger an IF statement, set constants for all the data, and append. Then move on to the next identifier IF statement
            const Identifiers = ['JoinTypeTrigger', 'AddFileTrigger']
            let i = 0
            console.log(SavesExecuteList)
            while (i < SavesExecuteList.length) {
                console.log(SavesExecuteList[i])
                if (SavesExecuteList[i] === "AddFileTrigger") {
                    let CurrentFileSelect = 'To Set'
                    i++ // On Selected File
                    i++ // On Item
                    CurrentFileSelect = SavesExecuteList[i]
                    console.log(CurrentFileSelect)
                    i++ // On End Selected File
                    let CurrentLeftJoinColumn = 'To Set'
                    i++ //On Left Key Start
                    i++ // On Item
                    CurrentLeftJoinColumn = SavesExecuteList[i]
                    console.log(CurrentLeftJoinColumn)
                    i++ // On Left Key End
                    let CurrentRightJoinColumn = 'To Set'
                    i++ // On Right Key Start
                    i++ // On Current Item
                    CurrentRightJoinColumn = SavesExecuteList[i]
                    console.log(CurrentRightJoinColumn)
                    i++ // On Right Key End
                    let CurrentDuplicateSelection = 'To Set'
                    i++ // On Duplicate Key Start
                    i++ // On Item
                    CurrentDuplicateSelection = SavesExecuteList[i]
                    console.log(CurrentDuplicateSelection)
                    i++ // On Duplicate Key Stop
                    i++ // On End Add File
                    i++ // Trigger Next Set
                    const CurrentIdentifier = cloneDeep(this.IdentifierGen)
                    const InitialDataObject = {'CurrentFileSelect': CurrentFileSelect, 'CurrentLeftJoinColumn': CurrentLeftJoinColumn,
                        'CurrentRightJoinColumn': CurrentRightJoinColumn, 'CurrentDuplicateSelection': CurrentDuplicateSelection
                    }
                    this.ComponentDataObject.push({'Component': 'SQLMergesData', 'identifier': this.IdentifierGen, 'InitialData': InitialDataObject, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
                    this.IdentifierGen++
                    this.$nextTick(() => {
                        setTimeout(() => { this.resetInitialData(CurrentIdentifier)}, 100)
                    })
                    continue
                }
                if (SavesExecuteList[i] === "JoinTypeTrigger") {
                    let CurrentJoinType = 'To Set'
                    i++ // On Join Type
                    i++ // On Current Item
                    CurrentJoinType = SavesExecuteList[i]
                    i++ // On End Join
                    i++ // On Join Type Trigger
                    i++ // Trigger next Set
                    const CurrentIdentifier = cloneDeep(this.IdentifierGen)
                    const InitialDataObject = {'CurrentJoinType': CurrentJoinType}
                    this.ComponentDataObject.push({'Component': 'SQLMergesType', 'identifier': this.IdentifierGen, 'InitialData': InitialDataObject, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
                    this.IdentifierGen++
                    this.$nextTick(() => {
                        setTimeout(() => { this.resetInitialData(CurrentIdentifier)}, 100)
                    })
                    continue
                }
            }
            console.log(Identifiers)

        },
        resetInitialData: function (val) {
            // Val = Identifier
            for (let i = 0; i < this.ComponentDataObject.length; i++) {
                if (this.ComponentDataObject[i]['identifier'] === val) { 
                        this.ComponentDataObject[i]['InitialData'] = {} // Reset Initial Data
                    }
            }
        },
        clearAll: function () {
            // Used to Clear All Component Data (NCN)
            console.log("In Clear All")
            this.ComponentDataObject = []
        },
        injectData: function () {
            console.log("In Inject Data")
            // Used to Trigger Components to Create an Executeables List (CN)
            try {
            if (this.ComponentDataObject[this.ComponentDataObject.length-1].Component !== "SQLMergesData") { // (CN Here)
                this.$buefy.snackbar.open({ 
                    message: 'The Last Block Must be a Data Selection Block',
                    type: 'is-warning',
                    position: 'is-top',
                })
                return false
            }
            this.ExecutionCounter = 0
            this.TriggerCheck = 'Check'
            setTimeout(() => { this.TriggerCheck = "No Check" }, 3000)
            } catch {
                console.log("No Elements Present")
            }
        },
        triggerData: function () {
            console.log("In Trigger Data")
            // Used to Post the Executeable List that is Gathered through this Emit Function (CN Everywhere)
            this.ExecutionCounter++
            if (this.ExecutionCounter === this.ComponentDataObject.length) {
                console.log("Reached the End of the Component Data Object")
                console.log(this.ComponentDataObject)
                //
                this.ExecuteList = []
                for (let i = 0; i < this.ComponentDataObject.length; i++) {
                    const EList = this.ComponentDataObject[i]['ExecuteList'] // Reset Initial Data
                    this.ExecuteList = this.ExecuteList.concat(EList)
                }
                console.log(this.ExecuteList)
                this.MostRecentExecutionData = cloneDeep(this.ExecuteList) // The Most Recent Cached Execution List independent of Exeuction List we Append to
                this.ExecuteList = [] // Need to Reset to Allow additional Appending
                this.ExecutionCounter = 0
                //
                const Identifiers = ['JoinTypeTrigger', 'AddFileTrigger']
                const IdentifierCheck = []
                const IdentifierTruthy = []
                //
                //console.log(this.MostRecentExecutionData)
                //console.log(this.ExecutionData)
                //
                for (let i = 0; i < this.MostRecentExecutionData.length; i++) {
                    if (this.MostRecentExecutionData.includes('Component Error')) { 
                            console.log("Component Error")
                            this.ExecuteList = []
                            this.MostRecentExecutionData = []
                            return false
                        }
                }
                console.log(this.MostRecentExecutionData, "Execution Data")
                for (let i = 0; i < this.MostRecentExecutionData.length; i++) {
                    if (Identifiers.includes(this.MostRecentExecutionData[i])) { 
                            IdentifierCheck.push(this.MostRecentExecutionData[i])
                        }
                }
                console.log(IdentifierCheck)
                for (let i = 0; i < IdentifierCheck.length; i++) {
                    // Used to set Error Message Prop Data Passed Down to Components
                    try {
                        if (this.CurrentBaseTableSelect === "" || this.CurrentBaseTableSelect === null || this.CurrentBaseTableSelect == undefined) {
                            this.$buefy.snackbar.open({ 
                                message: 'Base Table Not Selected - Failure',
                                type: 'is-warning',
                                position: 'is-top',
                            })
                            IdentifierTruthy.push(false)
                        }
                        if (IdentifierCheck[i-1] === 'JoinTypeTrigger' && IdentifierCheck[i] === 'JoinTypeTrigger') { 
                            this.ComponentDataObject[i]['ModuleErrors'] = 'Error: A Data Block Must Follow a Type Selection Block'
                            this.ComponentDataObject[i]['TriggerModuleError'] = true
                            setTimeout(() => { 
                                this.ComponentDataObject[i]['ModuleErrors'] = ''
                                this.ComponentDataObject[i]['TriggerModuleError'] = false}, 3000)
                            IdentifierTruthy.push(false)
                        }
                        if (IdentifierCheck[i-1] === 'AddFileTrigger' && IdentifierCheck[i] === 'AddFileTrigger') { 
                            this.ComponentDataObject[i]['ModuleErrors'] = 'Error: A Type Block Must either be First or Follow a Data Block'
                            this.ComponentDataObject[i]['TriggerModuleError'] = true
                            setTimeout(() => { 
                                this.ComponentDataObject[i]['ModuleErrors'] = ''
                                this.ComponentDataObject[i]['TriggerModuleError'] = false}, 3000)
                            IdentifierTruthy.push(false)
                        }
                    } catch {
                        console.log("Last Executable")
                    }
                }
                if (IdentifierTruthy.includes(false)) {
                    this.ExecuteList = []
                    this.MostRecentExecutionData = []
                    return false
                } else {
                    document.body.classList.add("loading");
                    this.$el.querySelector(".centerScreenText").classList.remove("hideCenterText")
                    //
                    const ExecutionDataSort = []
                    const ToggleDataList = []
                    // Base table first
                    // Does not Include Execution Data Unless Execution Data Updates a Component, in which a Selected File will be Assigned and Queried Upon
                    console.log("Got to Loading and Sorting Data ***")
                    ExecutionDataSort.push(this.CurrentBaseTableSelect)
                    for (let i = 0; i < this.ComponentDataObject.length; i++) {
                        const DataName = this.ComponentDataObject[i]['SelectedFile']
                        ExecutionDataSort.push(DataName)
                    }
                    for (let i = 0; i < ExecutionDataSort.length; i++) {
                        const DataName = ExecutionDataSort[i]
                        for (let i = 0; i < this.$store.state.ToggledDataList.length; i++) {
                            const FileName = this.$store.state.ToggledDataList[i]['UniqueFileName']
                            if (FileName === DataName) {
                                ToggleDataList.push(this.$store.state.ToggledDataList[i])
                            }
                        }
                    }
                    console.log(ToggleDataList)
                    //
                    const formData = new FormData(); // Setting the formData
                    formData.append('ModuleType', this.Module);
                    formData.append('ExecuteList', JSON.stringify(this.MostRecentExecutionData));
                    formData.append('FileLikeData', JSON.stringify(cloneDeep(ToggleDataList)));
                    console.log("Toggeled Data", this.$store.state.ToggledDataList)
                    if (this.$store.state.DataOptions.SaveExecution === true && this.ExecutionDataSaveName !== "") {
                        formData.append('DataSaveName', this.ExecutionDataSaveName);
                    }
                    if (this.$store.state.DataOptions.SaveExecution === true && this.ExecutionDataSaveName == "") {
                            this.$buefy.snackbar.open({
                                message: "Save Execution Data Toggeled with No Save Name - Execution Processing, Saving Cancelled",
                                type: "is-danger",
                                position: 'is-top',
                                actionText: 'Close',
                                indefinite: false
                            })
                    }
                    axios({
                        method: 'post',
                        url: '/ModuleBackendExecuteAll/',
                        cancelToken: cancelTokenSource.token,
                        data: formData,
                            headers: {
                                'content-type': 'multipart/form-data',
                            }
                    }).then((response) => {
                        document.body.classList.remove("loading");
                        this.$el.querySelector(".centerScreenText").classList.add("hideCenterText")
                        if (response.data['Response'] == "Success") {
                            this.SuccessHasTriggered = true
                            this.$buefy.snackbar.open({
                                message: "Execution Items Successful",
                                type: "is-success",
                                position: 'is-top',
                                actionText: 'Close',
                                indefinite: false
                            })
                            const BaseTableData = response.data['BaseTableData']
                            const BaseTableColumns = response.data['BaseTableColumns']
                            const RemovedTableData = response.data['RemovedTableData']
                            const RemovedTableColumns = response.data['RemovedTableColumns']
                            //
                            /* eslint-disable */
                            let DataSaveName = "To Set"
                            if (this.ExecutionDataSaveName === '' || this.ExecutionDataSaveName === null) {
                                DataSaveName = 'Executon Data'
                            } else {
                                DataSaveName = cloneDeep(this.ExecutionDataSaveName).toString()
                            }
                            const DataSaveNameColumns = DataSaveName + ' Columns'
                            const DataSaveNameRows = DataSaveName + ' Data'
                            const DataSaveNameRemovedColumns = DataSaveName + ' Removed Columns'
                            const DataSaveNameRemovedRows = DataSaveName + ' Removed Data'
                            const DataSaveNameNoSheets = DataSaveName + ' No Sheet Selection'
                            const DataSaveNameRemovedSheets = DataSaveName + ' Removed No Sheet Selection'
                            //
                            // You can't set a Key as a Variable unless you use ES6 Syntax, see here: https://stackoverflow.com/questions/2274242/how-to-use-a-variable-for-a-key-in-a-javascript-object-literal/2274327#2274327
                            const BasePush = {[DataSaveNameNoSheets]: {[DataSaveNameColumns]: BaseTableColumns, [DataSaveNameRows]: BaseTableData, 'Response': 'Success'}}
                            console.log(BasePush)
                            this.$store.state.ExecutionDataFileObjects = []
                            this.$store.state.ExecutionDataFileObjectsRemoved = []
                            this.$store.state.ExecutionDataFileObjects.push(BasePush)
                            if (this.MutlipleSelect === false) {
                                const RemovedPush = {[DataSaveNameRemovedSheets]: {[DataSaveNameRemovedColumns]: RemovedTableColumns, [DataSaveNameRemovedRows]: RemovedTableData, 'Response': 'Success'}}
                                this.$store.state.ExecutionDataFileObjectsRemoved.push(RemovedPush)
                            }
                            console.log("ExecutionFiles", this.$store.state.ExecutionDataFileObjects)
                            console.log("ExecutionFiles", this.$store.state.ExecutionDataFileObjectsRemoved)
                            //
                            /* eslint-disable */
                            this.SetExecutionData = true
                            this.$nextTick(() => {
                                setTimeout(() => { this.SetExecutionData = false }, 100)
                            })
                        } else if (response.data['Response'] == "Error") {
                        this.$buefy.snackbar.open({
                            message: "Error:" + response.data['Error'],
                            type: "is-danger",
                            position: 'is-top',
                            actionText: 'Close',
                            indefinite: false
                        })
                        } else {
                            this.$buefy.snackbar.open({
                                message: response.data['Response'],
                                type: "is-danger",
                                position: 'is-top',
                                actionText: 'Close',
                                indefinite: false
                            })
                        }
                    }).catch((error) => {
                        document.body.classList.remove("loading");
                        console.log(error)
                        this.$buefy.snackbar.open({
                            message: "Critical Error - Please Contact Support if Cancel Not Enacted",
                            type: "is-danger",
                            position: 'is-top',
                            actionText: 'Close',
                            indefinite: false
                        })
                            document.body.classList.remove("loading");
                            this.$el.querySelector(".centerScreenText").classList.add("hideCenterText")
                            this.Success = false
                    })
                }
            }
        },
        removeBlock:  function(val) {
            // Used as an Emit to Remove a Component Block (NCN)
            try{
                for (let i = 0; i < this.ComponentDataObject.length; i++) {
                    if (this.ComponentDataObject[i].identifier === val.identifier) { 
                        this.ComponentDataObject.splice(i, 1);
                        break;
                        }
                }
            } catch {console.log("Removal Error")}
        },
        addComponent:  function(val) {
            // Used to Add a Component Based on the Value Emitted from the Component Itself (CN)
            console.log(val)
            const Index = val.Index + 1
            const Component = val.Component
            if(Component === "SQLMergesData") {
                this.ComponentDataObject.splice(Index, 0, {'Component': 'SQLMergesData', 'identifier': this.IdentifierGen, 'InitialData': {}, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
            }
            if(Component === "SQLMergesType") {
                this.ComponentDataObject.splice(Index, 0, {'Component': 'SQLMergesType', 'identifier': this.IdentifierGen, 'InitialData': {}, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
            }
            this.$buefy.snackbar.open({
                message: 'Please Remember that Appending Mid-List Still Permits Invalid-Order Errors',
                type: 'is-warning',
                position: 'is-top',
            })
            this.IdentifierGen++
        },
        appendFirst() {
            // Used an a way to append a Block at the Beginning of the Component List (CN)
            this.ComponentDataObject.unshift({'Component': 'SQLMergesType', 'identifier': this.IdentifierGen, 'InitialData': {}, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
            this.IdentifierGen++
            this.$buefy.snackbar.open({
                message: 'Please Remember Criteria Blocks Can Not Follow Other Criteria Blocks',
                type: 'is-warning',
                position: 'is-top',
            })
        },
        appendJoinType() {
            // Used to Append Execute Blocks (CN)
            try {
            if (this.ComponentDataObject[(this.ComponentDataObject).length-1].Component === 'SQLMergesType') {
                this.$buefy.snackbar.open({
                    message: 'Error: A Join Type Block Must Either be First or Follow a Join Data Block',
                    type: 'is-warning',
                    position: 'is-top',
                })
                return false}
            } catch {
                // Good
            }
            this.ComponentDataObject.push({'Component': 'SQLMergesType', 'identifier': this.IdentifierGen, 'InitialData': {}, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
            this.IdentifierGen++
        },
        appendJoinData() {
            // Used to Append Criteria Blocks (CN)
            console.log("appendCriteria")
            try {
            if (this.ComponentDataObject[(this.ComponentDataObject).length-1].Component === 'SQLMergesData') {
                this.$buefy.snackbar.open({
                    message: 'Error: A Join Data Block Must Come after a Join Type Block',
                    type: 'is-warning',
                    position: 'is-top',
                })
                return false}
            } catch {
                this.$buefy.snackbar.open({
                    message: 'Error: The First Block Must be a Join Type Block',
                    type: 'is-warning',
                    position: 'is-top',
                })
                return false
            }
            this.ComponentDataObject.push({'Component': 'SQLMergesData', 'identifier': this.IdentifierGen, 'InitialData': {}, 'ModuleErrors': null, 'TriggerModuleError': false, 'SelectedFile': null, 'ExecuteList': null})
            this.IdentifierGen++
        },
        cancelCall() {
            cancelTokenSource.cancel();
            cancelTokenSource = axios.CancelToken.source();
            document.body.classList.remove("loading");
            this.$el.querySelector(".centerScreenText").classList.add("hideCenterText")
        },
        showAlert() {
            // Test for using Sweet Alert
            this.$swal({
            title: '<i class = "fas fa-align-justify DemoIcon" style = "transform: scale(1); height: 50px; width: 50px"/>',
            html:
                '<h1 class="title"> Module Specific Help </h1>' +
                'You can use <b>bold text</b>, ' +
                '<a href="//sweetalert2.github.io">links</a> ' +
                'and other HTML tags',
            });
        },
    },
    mounted() {
        // Used to Adjust the Width of the Columns Class on Mount
        // Check for Login
        this.$store.checkLogin();
        //
        console.log("Mounted")
        console.log(document.body.clientWidth)
        if (document.body.clientWidth <= 769) {
            console.log("True")
            this.columnsClass = false
        } else {
            this.columnsClass = true
        }

    }
}
</script>


<style lang="scss">
    @import "~ag-grid-community/dist/styles/ag-grid.css";
    @import "~ag-grid-community/dist/styles/ag-theme-alpine.css";
/*     
::-webkit-scrollbar{
    width: 10px;
    height: 10px;
}

::-webkit-scrollbar-thumb{
    border-radius:10px;
    height: 30px;
    width: 8px;
    border: 1px solid rgb(182, 177, 177);
    background: rgb(182, 177, 177);
}

::-webkit-scrollbar-track{
    background: transparent !important;
}

*/

.ag-body-horizontal-scroll {
    background: transparent !important;
}
.ag-body-horizontal-scroll-viewport {
    background: transparent !important;
}
// I think the spacers did the trick, they triggered a render?
.ag-horizontal-left-spacer {
    min-width: 1px !important;
}
.ag-horizontal-right-spacer {
    min-width: 1px !important;
}   

.ag-root-wrapper {
    border-radius: 15px !important;
    border: none !important;
}
.ag-theme-alpine .ag-row-odd {
    background-color: #eeeef7;
}

.ag-header-viewport {
    background-color: #cfcfdb !important;
}

.ag-theme-alpine .ag-header-row {
    background-color: #cfcfdb !important;
    border-bottom: 10px !important;
    border-color: solid black
}

.ag-theme-alpine .ag-header {
    border-color: none !important;
}
.card {
    border-radius: 15px !important;
}
.baseAg {
     overflow: auto !important;
     width: 100% !important;
}
.titlesales {
	font-size: 1.5em !important;
}
.slightround {
	border-radius: 1rem !important;
}

.DemoIcon {
    background-image:url("https://drive.google.com/uc?export=view&id=15pAcoh78KVR4H6ldbU7_7_Ie0XiWqs7H") !important;
    color: transparent !important;
    background-size: cover;
}
.BDMIcon {
    background-image:url('https://drive.google.com/uc?export=view&id=13G7Yx4cfgVrLiqQ65qETf7HKix2hSykz');
    color: transparent !important;
    padding-right: .6rem !important;
    padding-bottom: .1rem !important;
    background-size: cover;
}
.BDMIconTwo {
    background-image:url('https://drive.google.com/uc?export=view&id=1UUKO4A_ajEFAguI3O5r2rgUXUiFCXKyX');
    color: transparent !important;
    padding-right: .6rem !important;
    padding-bottom: .1rem !important;
    background-size: cover;
}
.CardIconCancel {
    background-image:url('https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/OOjs_UI_icon_trash-destructive.svg/1200px-OOjs_UI_icon_trash-destructive.svg.png');
    color: transparent !important;
    padding-right: .6rem !important;
    padding-bottom: .1rem !important;
    background-size: cover;
}
.CardIconGo {
    background-image:url('https://drive.google.com/uc?export=view&id=10KxUMau26Z8_vp97U490l26Lb7iR9a7y');
    color: transparent !important;
    padding-right: .6rem !important;    
    padding-bottom: .1rem !important;
    background-size: cover;
}
.HeaderModule {
    background-color: white !important;
}
.CardModule {
    background-color: #8c67ef !important;
    border-radius: 1rem !important;
}
.veryrounded {
    border-radius: 30px !important;
}
.ButtonsDiv {
    background-color: lightseagreen !important;
}
.iconMargin {
    margin-top: .7rem !important;
}
.iconMarginLarge {
    margin-top: 1rem !important;
}
.OptionsBorderRadius {
    border-radius: 10px 10px 40px 40px !important;
}
.text-break {
    // this Nifty line causes long lines to break - USE THIS on LONG STRINGS
    white-space: normal !important;
    word-break: break-all !important;
}
.has-text-pink {
    color: pink !important;
}
.customHeader {
    background-color: transparent;
    align-items: stretch;
    box-shadow: 0 0.125em 0.25em rgba(10,10,10,.1);
}

.HeaderText {
        font-family: Slack-Larsseit,"Helvetica Neue",Helvetica,"Segoe UI",Tahoma,Arial,sans-serif;
}

.hoverexpandSmall {
    transition: transform 0.2s
}
.hoverexpandSmall:hover {
    transform: scale(1.02)
}
.smallIcon {
    min-width: 1.5rem !important;
    min-height: 1.5rem !important;
}
.nodisplay {
    display: none !important;
}
</style>
