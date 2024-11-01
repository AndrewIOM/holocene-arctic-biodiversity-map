open BiodiversityCoder.Core
open BiodiversityCoder.Core.GraphStructure
open BiodiversityCoder.Core.FieldDataTypes
open RProvider
open RProvider.Operators
open RProvider.oxcAAR

/// The standard deviation to apply to a date
/// where there was no standard deviatin specified
/// in the text / coded into the graph.
let defaultSdWhenUnspecified = 100.<OldDate.calYearBP>

let analysisDate = Time.SimpleDateOnly.TryCreate(Text (System.DateOnly.FromDateTime(System.DateTime.Now).ToShortDateString())) |> Option.get
let analysisPerson = "Martin, A.C." |> Author.create |> Result.forceOk


/// Queries the graph database to obtain timelines for which
/// we have at leaat two individual dates. Returns the timeline
/// ID * a list of individual dates.
let queryGraphForDates (graph:Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation>) =
    result {

        let! sourceIds = 
            graph.Nodes<Sources.SourceNode> ()
            |> Result.ofOption "Could not load sources"

        // Find temporal extents from sources
        let! sourceAtoms =
            sourceIds
            |> Seq.map(fun kv -> kv.Key)
            |> Seq.toList
            |> Storage.loadAtoms graph.Directory (typeof<Sources.SourceNode>).Name

        let! allTimelines =
            sourceAtoms
            |> List.collect(fun (s:Graph.Atom<GraphStructure.Node, GraphStructure.Relation>) -> 
                (snd s)
                |> List.choose(fun (_,sinkId,_,conn) ->
                    match conn with
                    | GraphStructure.Relation.Source r ->
                        match r with
                        | Sources.SourceRelation.HasTemporalExtent -> Some sinkId
                        | _ -> None
                    | _ -> None )
            )
            |> Storage.loadAtoms graph.Directory (typeof<Exposure.ExposureNode>.Name)

        let! datesByTimelineId =
            allTimelines
            |> List.map(fun (timelineNode,timelineRelations) ->
                timelineNode,
                timelineRelations
                |> List.choose(fun (_,sinkId,_,conn) ->
                    match conn with
                    | Relation.Exposure r ->
                        match r with
                        | Exposure.ExposureRelation.ConstructedWithDate -> Some sinkId
                        | _ -> None
                    | _ -> None ))
            |> List.filter(fun (_,l) -> l.Length > 1)
            |> List.map(fun (node, dates) ->

                dates
                |> Storage.loadAtoms graph.Directory (typeof<Exposure.StudyTimeline.IndividualDateNode>.Name)
                |> Result.map(fun (nodes: Graph.Atom<GraphStructure.Node,obj> list) ->
                    nodes
                    |> List.choose(fun ((k,n),_) ->
                        match n with
                        | GraphStructure.Node.ExposureNode p ->
                            match p with
                            | Exposure.ExposureNode.DateNode d -> Some (k,d)
                            | _ -> None
                        | _ -> None
                    )
                )
                |> Result.lift(fun dates -> node |> fst, dates)
            )
            |> Result.ofList

        return datesByTimelineId
    }

let measurementError measureError =
    match measureError with
    | FieldDataTypes.OldDate.MeasurementError.DatingErrorPlusMinus sd -> sd
    | FieldDataTypes.OldDate.MeasurementError.DatingErrorPlusMinusSigma (_,sd) -> sd
    | FieldDataTypes.OldDate.MeasurementError.DatingErrorRangeSigma (_,low,hi) -> Seq.average [ low; hi ]
    | FieldDataTypes.OldDate.MeasurementError.NoDatingErrorSpecified -> defaultSdWhenUnspecified


module OxCal =

    open BiodiversityCoder.Core.Exposure.Reanalysis

    type OxCalInputDate =
        | RadiocarbonWithDepth of r:float<OldDate.uncalYearBP> * sd:float<OldDate.calYearBP> * depth:float<StratigraphicSequence.cm>
        | RadiocarbonNoDepth of r:float<OldDate.uncalYearBP> * sd:float<OldDate.calYearBP>
        | DateAD of ad:float<OldDate.AD>
        | DateBC of ad:float<OldDate.BC>
        | Tephra of ad:float * depth:float<StratigraphicSequence.cm>

    let adDate adDate depth = sprintf "Date(AD(%i)) { z=%f; };" adDate depth
    let radiocarbonDate year sd depth = sprintf """R_Date("",%i,%i){ z=%f; };""" year sd depth
    let radiocarbonDateNoDepth year sd = sprintf """R_Date("",%i,%i);""" year sd
    
    /// Set k0 as 1 when using cm or 100 when using metres.
    let oxCalSedimendarySequenceScript (dates:string seq) =
        let k0 = 1
        sprintf """ Plot() { P_Sequence("variable",%i,2,U(-2,2)) { { Boundary("Bottom"){}; %s; Boundary("Top") {}; };};""" k0 (dates |> String.concat ";\n")

    let calibrateDatesFromSequence dates =
        
        let oxCalScript : string =

            // Dates should be ordered bottom to top (depth-basis)...

            dates
            |> Seq.map(fun d ->
                match d with
                | RadiocarbonWithDepth (r,sd,depth) -> radiocarbonDate (int r) (int sd) depth
                | RadiocarbonNoDepth (r, sd) ->
                    printfn "Warning: mixed radiocarbon dates with and without depths"
                    radiocarbonDateNoDepth (int r) (int sd)
                    )
            |> oxCalSedimendarySequenceScript

        let my_result_file = R.executeOxcalScript(oxcal__script = oxCalScript)
        let my_result_text = R.readOxcalOutput(my_result_file)
        let my_result_data = R.parseOxcalOutput(my_result_text)

        // Age-depth model:
        let fullModel = R.parseFullOxcalOutput(my_result_text)
        let depths = fullModel?model?``element[1]``?age_depth_z.GetValue<float list>() |> List.map(fun f -> f * 1.<StratigraphicSequence.cm>)
        let ages = fullModel?model?``element[1]``?age_depth_mean.GetValue<float list>() |> List.map(fun f -> f * 1.<OldDate.calYearBP>)
        let agesSd = fullModel?model?``element[1]``?age_depth_sd.GetValue<float list>() |> List.map(fun f -> f * 1.<OldDate.calYearBP>)
        let calibrationCurve = fullModel?``calib[0]``?ref.GetValue<string> ()

        let ageDepthModel : Map<float<StratigraphicSequence.cm>, float<OldDate.calYearBP> * float<OldDate.calYearBP>> = 
            List.zip3 depths ages agesSd
            |> List.map(fun (d,a,sd) -> d, (a, sd))            
            |> Map.ofList

        (fun curve modelCode softwareName softwareVersion ->
            {
                CalibrationCurve = curve
                ModelApplied = OxCalModel modelCode
                SoftwareName = softwareName
                SoftwareVersion = softwareVersion
                Origin = OldDate.Harmonised.DateCalibrationOrigin.PartOfReanalysis(analysisPerson, analysisDate)
                AgeDepthModel = Some ageDepthModel
            })
        <!> (Text.createShort calibrationCurve)
        <*> (Text.create oxCalScript)
        <*> ("OxCal" |> Text.createShort)
        <*> ("4.4.4" |> Text.createShort)


// Script starts here

printfn "Setting up OxCal..."
R.quickSetupOxcal() |> ignore

printfn "Querying graph database for dated timelines..."

let graph =
    Storage.loadOrInitGraph "../../data/" |> Result.forceOk

let timelines = queryGraphForDates graph

////////////////////////////////////////
// Part 1: Individual dates / age-depth models
//////////////////////////////////////?/

match timelines with
| Error e -> printfn "Failed to read graph: %s." e
| Ok timelines ->
    printfn "Query identified %i timelines with individual dates." timelines.Length

    for t in timelines do

        // Find existing link to calibration and skip if found.

        // If there are dates not from depths, do not use a sequence model.
        // If all dates are depth-related, use a sequence model.

        // NB What about surface age?

        let preparedDates =
            (snd t) 
            |> List.filter(fun (_,date) -> not date.Discarded)
            |> List.map(fun (indDateKey,date) ->

                let depth =
                    match date.SampleDepth with
                    | Some d ->
                        match d with
                        | StratigraphicSequence.DepthInCore.DepthBand (low,up) -> Some <| Seq.average [ low.Value; up.Value ]
                        | StratigraphicSequence.DepthInCore.DepthNotStated -> None
                        | StratigraphicSequence.DepthInCore.DepthPoint p -> Some p.Value
                        | StratigraphicSequence.DepthInCore.DepthQualitativeLevel _ -> None
                    | None -> None

                let handleUncalDate (uncal:OldDate.UncalDate) depth =
                    match depth with
                    | Some d ->  Some <| OxCal.RadiocarbonWithDepth(uncal.Date, measurementError uncal.UncalibratedDateError, d)
                    | None -> Some <| OxCal.RadiocarbonNoDepth(uncal.Date, measurementError uncal.UncalibratedDateError)

                let handleCalDate (cal:OldDate.CalibratedRadiocarbonDate) depth =
                    match cal.UncalibratedDate with
                    | Some uncal -> handleUncalDate uncal depth
                    | None ->
                        printfn "Ignoring calibrated radiocarbon date that has no uncalibrated value."
                        None

                let handleOldDate d depth =
                    match d with
                    | OldDate.OldDate.CalYrBP cal -> handleCalDate cal depth
                    | OldDate.OldDate.BP bp ->
                        match depth with
                        | Some d ->  Some <| OxCal.RadiocarbonWithDepth(bp, measurementError date.MeasurementError, d)
                        | None -> Some <| OxCal.RadiocarbonNoDepth(bp, measurementError date.MeasurementError)
                    | OldDate.OldDate.HistoryYearAD ad -> Some <| OxCal.DateAD ad
                    | OldDate.OldDate.HistoryYearBC bc -> Some <| OxCal.DateBC bc


                indDateKey,
                depth,
                match date.Date with
                | OldDate.OldDatingMethod.RadiocarbonCalibrated cal -> handleCalDate cal depth
                | OldDate.OldDatingMethod.RadiocarbonCalibratedRanges ranges ->
                    ranges.UncalibratedDate |> Option.bind (fun u -> handleUncalDate u depth)
                | OldDate.OldDatingMethod.RadiocarbonUncalibrated bp
                | OldDate.OldDatingMethod.RadiocarbonUncalibratedConventional bp -> 
                    match depth with
                    | Some d ->  Some <| OxCal.RadiocarbonWithDepth(bp, measurementError date.MeasurementError, d)
                    | None -> Some <| OxCal.RadiocarbonNoDepth(bp, measurementError date.MeasurementError)
                | OldDate.OldDatingMethod.CollectionDate y -> Some <| OxCal.DateAD y
                | OldDate.OldDatingMethod.HistoricEvent (_, d)
                | OldDate.OldDatingMethod.Tephra (_,d) -> handleOldDate d depth
                | OldDate.OldDatingMethod.Lead210 _
                | OldDate.OldDatingMethod.DepositionalZone _
                | OldDate.OldDatingMethod.Radiocaesium _ -> None )

        let totalDates = preparedDates |> Seq.where (fun (_,_,d) -> d.IsSome) |> Seq.length
        if totalDates = 0
        then
            printfn "Skipping timeseries as there were no dates left after pre-processing."
            ()
        else

            let isDepthSequence = preparedDates |> List.map(fun (_,d,_) -> d) |> List.contains None |> not
            if isDepthSequence then

                let usedDateKeys, orderedDates =
                    preparedDates
                    |> List.sortByDescending(fun (_,d,_) -> d.Value)
                    |> List.filter(fun (_,_,d) -> d.IsSome)
                    |> List.map(fun (k,_,d) -> k,d)
                    |> List.unzip

                result {
                    let! calibrated = OxCal.calibrateDatesFromSequence (orderedDates |> List.choose id)

                    let! (g,addedNodes) = Storage.addNodes graph [ calibrated |> Exposure.ExposureNode.RecalibratedDateNode |> Node.ExposureNode ]
                    let newCalNode = addedNodes.Head

                    let linkDateToCalNode g indDateKey =
                        g |> Result.bind(fun g ->
                            Storage.addRelationByKey g indDateKey (newCalNode |> fst |> fst) (ProposedRelation.Exposure Exposure.ExposureRelation.UsedInCalibration)
                        )

                    let! graphWithInboundLinks = 
                        usedDateKeys
                        |> List.fold linkDateToCalNode (Ok g)

                    // TODO Link cal node back to dates with their sigma ranges:
                    // let linkCalNodeToDates g indDateKey =
                    //     g |> Result.bind(fun g ->
                    //         Storage.addRelationByKey g indDateKey (newCalNode |> fst |> fst) (ProposedRelation.Exposure Exposure.ExposureRelation.UsedInCalibration)
                    //     )


                    return ()
                } |> Result.forceOk
            else
                ()

/////////////////////////////////////////////////
// Part 2: Temporal extent (simple calibrations)
/////////////////////////////////////////////////

// TODO
// - Read in extents for all timelines
// - Where radiocarbon, recalibrate using OxCal (single date mode)
// - Link early and latest extent with relations into calyr nodes.
