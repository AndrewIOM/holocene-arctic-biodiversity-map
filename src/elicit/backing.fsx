#r "nuget:Newtonsoft.Json,v=13.0"
#r "nuget: FSharp.Data"
#r "nuget: Microsoft.FSharpLu.Json"
#r "../../dist/BiodiversityCoder.Core.dll"

open FSharp.Data
open System
open BiodiversityCoder.Core.Scenarios
open BiodiversityCoder.Core
open BiodiversityCoder.Core.FieldDataTypes
open System.Text.RegularExpressions
open BiodiversityCoder.Core.Population.BioticProxies
open BiodiversityCoder.Core.Exposure.StudyTimeline


type LocationData = {
    SiteName: Text.ShortText
    Origin: Population.Context.SampleOrigin
    Location: Geography.SamplingLocation
    OldestAge: OldDate.OldDateSimple
    YoungestAge: OldDate.OldDateSimple
    CoreDepthCm: int<StratigraphicSequence.cm> option
    Proxies: BioticProxyCategoryNode list
}

type ScenarioType =
    | Dendro of Scenarios.WoodRingScenario list
    | Simple of LocationData

module Extract =

    let (|RegexMatch|_|) pattern input =
        let regex = Regex pattern
        let m = regex.Match input
        if m.Success then
            Some [ for g in m.Groups -> g.Value ]
        else
            None

    let onlyPipesDashesAndWhite s =
        match s with
        | RegexMatch "^[|\- \013]*$" m -> true
        | _ -> false

    let tryParseDateField (dateField:string) (datePlusMinus:string) (sigmaField:string) =
        // What format has the date been given in?
        let dateAndSigma =
            match dateField.Trim() with
            | RegexMatch "([0-9]+) ± ([0-9]+)" m ->
                printfn "x %A" m
                let date = m.[1] |> float
                let sigma = m.[2] |> float |> (*) 1.<OldDate.calYearBP> |> Some
                Some (date, sigma)
            | i when Float.tryParse i |> Option.isSome ->
                printfn "y %A" i
                let date = Float.tryParse i |> Option.get
                let sigma =
                    match datePlusMinus with
                    | RegexMatch "^±([0-9]{1,4})$" m -> m.[1] |> float |> (*) 1.<OldDate.calYearBP> |> Some
                    | _ -> None
                Some (date, sigma)
            | _ -> None

        let sigmaLevel =
            printfn "Sigma field [%A]" sigmaField
            match sigmaField.Trim() with
            | RegexMatch "^([1-3]) ?(σ|sigma)" ms -> ms.[1] |> int |> Some
            | _ -> None

        dateAndSigma
        |> Option.map(fun (date, sigma) ->
            match sigmaLevel with
            | Some sl ->
                match sigma with
                | Some sigma ->
                    date,
                    match sl with
                    | 1 -> OldDate.DatingErrorPlusMinusSigma(OldDate.Sigma.OneSigma, sigma)
                    | 2 -> OldDate.DatingErrorPlusMinusSigma(OldDate.Sigma.TwoSigma, sigma)
                    | 3 -> OldDate.DatingErrorPlusMinusSigma(OldDate.Sigma.ThreeSigma, sigma)
                    | _ -> OldDate.NoDatingErrorSpecified
                | None -> date, OldDate.NoDatingErrorSpecified
            | None ->
                match sigma with
                | Some sigma -> date, OldDate.DatingErrorPlusMinus sigma
                | None -> date, OldDate.NoDatingErrorSpecified
        )

    /// Extract individual dates from a markdown table.
    let extractDates (dataDates:string) =
        printfn "dates %s" dataDates
        dataDates.Split "\n"
        |> Seq.tail // skip header
        |> Seq.skipWhile onlyPipesDashesAndWhite
        |> Seq.map(fun s -> s.Trim().Trim('|') .Split "|" |> Array.map (fun s -> s.Trim()))
        |> Seq.map(fun row ->

            let depth =
                match row.[1].Trim().ToLower() with
                | RegexMatch "^([0-9]{1,4}) ?[-–] ?([0-9]{1,4})" depths ->
                    let d =
                        [ float depths.[1] |> StratigraphicSequence.createDepth |> forceOk
                          float depths.[2] |> StratigraphicSequence.createDepth |> forceOk ]
                    Some <| StratigraphicSequence.DepthInCore.DepthBand (Seq.max d, Seq.min d)
                | i when Float.tryParse i |> Option.isSome ->
                    Float.tryParse i
                    |> Option.bind (StratigraphicSequence.createDepth >> Result.toOption)
                    |> Option.map StratigraphicSequence.DepthInCore.DepthPoint
                | _ -> None

            printfn "Depth for date = %A" depth
            printfn "date = %A" row

            let date =
                match row.[0].ToLower().Trim() with
                | RegexMatch "radiocarbon" _ ->
                
                    let dateWithUncertainty = tryParseDateField row.[3] row.[4] row.[6]
                    let dateWithUncertaintyUncal = tryParseDateField row.[8] row.[9] row.[6]
                    
                    // Is it calibrated?
                    match row.[5].Trim().ToLower() with
                    | RegexMatch "^not" _
                    | RegexMatch "^-$" _ ->
                        // Not calibrated
                        printfn "not calibrated"
                        match dateWithUncertainty with
                        | Some (date, uncertainty) ->
                            Some (date |> (*) 1.<OldDate.uncalYearBP> |> OldDate.OldDatingMethod.RadiocarbonUncalibrated, uncertainty)
                        | None -> None
                    | _ ->
                        printfn "calibrated"
                        printfn "%A > %A" dateWithUncertainty dateWithUncertaintyUncal
                        match dateWithUncertainty with
                        | None -> None
                        | Some (date, uncertainty) ->
                            let calMethod =
                                match row.[5] with
                                | RegexMatch "^not" _ -> "Unknown" |> Text.createShort |> forceOk
                                | _ -> row.[7] |> Text.createShort |> forceOk

                            Some (
                                OldDate.OldDatingMethod.RadiocarbonCalibrated
                                    {
                                        CalibratedDate = date |> (*) 1.<OldDate.calYearBP>
                                        CalibrationCurve = calMethod
                                        UncalibratedDate = dateWithUncertaintyUncal |> Option.map(fun (d,e) ->  { UncalibratedDateError = e; Date = d * 1.<OldDate.uncalYearBP> })
                                    }, uncertainty)
                | _ -> None

            let labNumber =
                match row.[7] with
                | RegexMatch "^not" _ -> None
                | _ -> Some (row.[7] |> Text.createShort |> forceOk)

            let materialDated =
                match row.[7] with
                | RegexMatch "^not" _ -> "Unknown" |> Text.createShort |> forceOk
                | _ -> row.[10] |> Text.createShort |> forceOk

            date |> Option.map(fun (date, measureError) ->
                row.[2], {
                    Date = date
                    MeasurementError = measureError
                    MaterialDated = materialDated
                    SampleDepth = depth
                    LabNumber = labNumber
                    Discarded = false
                }
            )
        )
        |> Seq.toList

    let extractDepths (dataDepths:string) =
        printfn "Data depth extract = %s" dataDepths
        dataDepths.Split "\n"
        |> Seq.tail
        |> Seq.skipWhile onlyPipesDashesAndWhite
        |> Seq.map(fun s -> s.Split "|" |> Array.map (fun s -> s.Trim())  |> Array.filter(fun s -> s <> ""))
        // |> Seq.map(fun s -> s.Split "|" |> Array.except [""])
        |> Seq.map(fun row ->
            printfn "Depth extract row = %A" row
            let asCm (str: string) = str |> float |> StratigraphicSequence.createDepth |> forceOk
            let depths =
                match row.[4].Trim(), row.[3].Trim() with
                | RegexMatch "([0-9]{1,4}) cm" m1, RegexMatch "([0-9]{1,4}) cm" m2 ->
                    StratigraphicSequence.DepthExtent.DepthRange (m2.[1] |> asCm, m1.[1] |> asCm)
                | i1, i2 when Float.tryParse i1 |> Option.isSome && Float.tryParse i1 |> Option.isSome ->
                    StratigraphicSequence.DepthExtent.DepthRange (i2 |> asCm, i1 |> asCm)
                | _ -> StratigraphicSequence.DepthExtent.DepthRangeNotStated
            
            {|
                Site = row.[0]
                Depths = depths
            |}
        )
        |> Seq.toList

    let isCalibrated (calibrationMethod: string) =
        match calibrationMethod.ToLower().Trim().Replace(" ", "") with
        | "notspecified" -> None
        | _ -> calibrationMethod |> Text.createShort |> Result.toOption

    let extractCoreLocations (depths:list<{| Depths: StratigraphicSequence.DepthExtent; Site: string |}>) (dataLocation:string) =
        dataLocation.Split "\n"
        |> Seq.tail
        |> Seq.skipWhile onlyPipesDashesAndWhite
        |> Seq.map(fun s -> s.Split "|" |> Array.map (fun s -> s.Trim())  |> Array.filter(fun s -> s <> ""))
        // |> Seq.map(fun s -> s.Split "|" |> Array.except [""])
        |> Seq.map(fun row ->
            
            result {
                printfn "Row is %A" row
                let! siteName = row.[0].Trim() |> Text.createShort

                let sampleOrigin depths =
                    match row.[1].Trim() with
                    | s when s.ToLower().Contains "tree" -> Population.Context.LivingOrganism
                    | s when s.ToLower().Contains "peat" -> Population.Context.PeatCore depths
                    | s when s.ToLower().Contains "sediment" -> Population.Context.LakeSediment depths
                    | _ -> Population.Context.OtherOrigin (row.[1].Trim() |> Text.createShort |> forceOk, Some depths)

                let! location =
                    let lat, lon = row.[3].Replace(" ", ""), row.[4].Replace(" ", "")
                    match lat, lon with
                    | RegexMatch "([0-9]{1,3}\.[0-9]{1,10})°?N" m1, RegexMatch "([0-9]{1,3}\.[0-9]{1,10})°?([EW])+" m2
                    | RegexMatch "([0-9]{1,3})°?N" m1, RegexMatch "([0-9]{1,3})°?([EW])+" m2 ->
                        let lat =
                            m1.[1]
                            |> Float.tryParse
                            |> Result.ofOption "Latitude is not a number"
                            |> Result.bind Geography.createLatitude
                        let lon =
                            m2.[1]
                            |> Float.tryParse
                            |> Result.ofOption "Longitude is not a number"
                            |> Result.map(fun f -> if m2.[2] = "W" then f * -1. else f)
                            |> Result.bind Geography.createLongitude
                        printfn "LLL %A, %A" lat lon
                        match lat, lon with
                        | Ok l, Ok lo -> Ok <| Geography.Site (l,lo)
                        | _ -> Error "Could not make DD coordinate"
                    | RegexMatch "^([0-9]{1,2})[:|°]([0-9]{1,2}(?:\.[0-9]+){0,1})[:|'|′]?([N|S])" _, _ ->
                        printfn "TEST 1353"
                        Geography.createCoordinateDDM (lat.Trim() + "," + lon.Trim()) |> Result.map Geography.SiteDDM
                    | RegexMatch "^([0-9]{1,2})[:|°]([0-9]{1,2})[:|'|′]?([0-9]{1,2}(?:\.[0-9]+){0,1})?[\"|″]([N|S])" _, _ ->
                        printfn "COOL %s" (lat.Trim() + "," + lon.Trim())
                        Geography.createCoordinate (lat.Trim() + "," + lon.Trim()) |> Result.map Geography.SiteDMS
                    | RegexMatch "Notspecified" _, _
                    | RegexMatch "Notmentioned" _, _ ->
                        Error (sprintf "Latitude / Longitude was unspecified (%s)" siteName.Value)
                    | _ ->
                        printfn ":Catchall %A %A" (lat.Trim()) (lon.Trim())
                        let lat =
                            lat.Trim()
                            |> Float.tryParse
                            |> Result.ofOption "Latitude is not a number"
                            |> Result.bind Geography.createLatitude
                        let lon =
                            lon.Trim()
                            |> Float.tryParse
                            |> Result.ofOption "Longitude is not a number"
                            |> Result.bind Geography.createLongitude
                        printfn "LAT LON %A %A" lat lon
                        match lat, lon with
                        | Ok l, Ok lo -> Ok <| Geography.Site (l,lo)
                        | _ -> Error "Could not make DD coordinate"

                // If 'modern' or 'present', use collection date or 0ybp as the age.
                let! youngestAge =
                    let y = row.[6].Trim().ToLower()
                    if y.Contains "modern" || y.Contains "present" || y.Contains "0 ka" || y.Contains "recent"
                    then
                        Float.tryParse row.[2]
                        |> Option.map(fun y -> OldDate.OldDateSimple.HistoryYearAD <| y * 1.<OldDate.AD>)
                        |> Option.defaultValue (0.<OldDate.uncalYearBP> |> OldDate.OldDateSimple.BP)
                        |> Some
                    else
                        match y with
                        | RegexMatch "([0-9]{4}) +ce" p
                        | RegexMatch "([0-9]{4})" p ->
                            p.[1] |> Float.tryParse |> Option.map(fun y -> y * 1.<OldDate.AD> |> OldDate.OldDateSimple.HistoryYearAD)
                        | RegexMatch "([,0-9]+) bp" m -> 
                            Float.tryParse m.[1] |> Option.map(fun y -> y * 1.<OldDate.uncalYearBP> |> OldDate.OldDateSimple.BP)
                        | RegexMatch "([,0-9]+) cal yr bp" m ->
                            Float.tryParse m.[1] |> Option.map(fun y -> (y * 1.<OldDate.calYearBP>, None) |> OldDate.OldDateSimple.CalYrBP)
                        | RegexMatch "([.,0-9]+) cal ka bp" m -> 
                            Float.tryParse m.[1] |> Option.map(fun y -> (y * 1.<OldDate.calYearBP> * 1000., None) |> OldDate.OldDateSimple.CalYrBP)
                        | _ -> None
                    // |> Option.map(
                    //     function
                    //     | RegexMatch "Notspecified" _
                    //     | RegexMatch "Notmentioned" _ ->
                    //         Error ""
                        
                    //     )
                    |> Result.ofOption "Could not parse youngest age"

                // Figure out if calibrated or not.
                let isCal = isCalibrated row.[7]

                let! oldestAge =
                    let o = row.[5].Trim().ToLower()
                    printfn "O = %A" o
                    match o with
                    | RegexMatch "([0-9]{1,4}) a.?d.?" p
                    | RegexMatch "a.?d.? ([0-9]{1,4})" p ->
                        p.[1] |> Float.tryParse |> Option.map(fun y -> y * 1.<OldDate.AD> |> OldDate.OldDateSimple.HistoryYearAD)
                    | RegexMatch "bc ([0-9]{1,4})" p
                    | RegexMatch "([0-9]{1,4}) bc" p ->
                        p.[1] |> Float.tryParse |> Option.map(fun y -> y * 1.<OldDate.BC> |> OldDate.OldDateSimple.HistoryYearBC)
                    | RegexMatch "([,0-9]+) cal\.? yr bp" m -> 
                        Float.tryParse m.[1] |> Option.map(fun y -> (y * 1.<OldDate.calYearBP>, None) |> OldDate.OldDateSimple.CalYrBP)
                    | RegexMatch "([.,0-9]+) cal\.? ka bp" m -> 
                        Float.tryParse m.[1] |> Option.map(fun y -> (y * 1.<OldDate.calYearBP> * 1000., None) |> OldDate.OldDateSimple.CalYrBP)
                    | RegexMatch "([,0-9]+) bp" m -> 
                        Float.tryParse m.[1] |> Option.map(fun y -> (y * 1.<OldDate.uncalYearBP>) |> OldDate.OldDateSimple.BP)
                    | RegexMatch "([.,0-9]+) ka bp" m -> 
                        Float.tryParse m.[1] |> Option.map(fun y -> y * 1.<OldDate.uncalYearBP> * 1000. |> OldDate.OldDateSimple.BP)
                    | RegexMatch "([,.0-9]+) ?y" m ->
                        Float.tryParse m.[1] |> Option.map(fun y ->
                            match isCal with
                            | Some cal -> (y * 1.<OldDate.calYearBP>, Some cal) |> OldDate.OldDateSimple.CalYrBP
                            | None -> (y * 1.<OldDate.uncalYearBP>) |> OldDate.OldDateSimple.BP )
                    | RegexMatch "([,.0-9]*) ?ka" m -> 
                        Float.tryParse m.[1] |> Option.map(fun y ->
                            match isCal with
                            | Some cal -> (y * 1000.<OldDate.calYearBP>, Some cal) |> OldDate.OldDateSimple.CalYrBP
                            | None -> (y * 1000.<OldDate.uncalYearBP>) |> OldDate.OldDateSimple.BP )
                    | _ -> None
                    |> Result.ofOption "Could not parse oldest age"

                let proxyCategories =
                    row.[9].Split "," |> Array.map(fun p ->
                        match p.ToLower() with
                        | RegexMatch "pollen" _ -> Microfossil Pollen
                        | RegexMatch "macrofossil" _ -> Microfossil PlantMacrofossil
                        | RegexMatch "macrofossil" _ -> Microfossil Diatom
                        | RegexMatch "ostracod" _ -> Microfossil Ostracod
                        | RegexMatch "chironomid" _ -> Microfossil (OtherMicrofossilGroup <| forceOk (Text.createShort "Chironomid"))
                        | _ -> OtherProxy (p |> Text.createShort |> forceOk)
                    )

                let coreDepth =
                    match row.[8].Trim() with
                    | "NA" -> None
                    | _ ->
                        printfn "N = '%A'" (row.[8].Trim().Replace("cm", ""))
                        row.[8].Trim().Replace("cm", "")
                        |> Float.tryParse
                        |> Option.map (System.Math.Round >> int >> (*) 1<StratigraphicSequence.cm>)
                printfn "Row = %A; depths = %A" row depths
                let treeSpecies = row.[10].Trim().Split "," |> Array.toList |> List.except [ "NA" ]

                let getDepths () =
                    printfn "Site %A, depths = %A" siteName.Value depths
                    depths
                    |> Seq.tryFind(fun d -> d.Site = siteName.Value)
                    |> Result.ofOption (sprintf "Depths not available for: %A" siteName.Value)

                let oldestAgeAd =
                    match oldestAge with
                    | OldDate.OldDateSimple.BP _ -> nan * 1.<OldDate.AD>
                    | OldDate.OldDateSimple.CalYrBP _ -> nan * 1.<OldDate.AD>
                    | OldDate.OldDateSimple.HistoryYearAD a -> a
                    | OldDate.OldDateSimple.HistoryYearBC b -> nan * 1.<OldDate.AD>

                let youngestAgeAd =
                    match youngestAge with
                    | OldDate.OldDateSimple.BP _ -> nan * 1.<OldDate.AD>
                    | OldDate.OldDateSimple.CalYrBP _ -> nan * 1.<OldDate.AD>
                    | OldDate.OldDateSimple.HistoryYearAD a -> a
                    | OldDate.OldDateSimple.HistoryYearBC b -> nan * 1.<OldDate.AD>

                let collectionDate =
                    Float.tryParse row.[2]
                    |> Option.map(fun y -> y * 1.<OldDate.AD>)
                    |> Option.orElse (Some youngestAgeAd)

                let! treeSpeciesDendro =
                    treeSpecies
                    |> List.map Text.createShort
                    |> Result.ofList
                    |> Result.map(fun l -> l |> List.map(fun s -> Scenarios.WoodTaxon.Genus (s)))

                return!
                    if (sampleOrigin StratigraphicSequence.DepthRangeNotStated).IsLivingOrganism
                    then
                        let! collectedDate =
                            collectionDate
                            |> Result.ofOption "Could not parse collection date"
                        collectedDate |> Result.map(fun colDate ->
                            treeSpeciesDendro
                            |> List.map(fun taxon ->
                                {
                                    CreateTaxon = false
                                    SiteName = siteName
                                    Location = location
                                    EarliestYear = oldestAgeAd
                                    LatestYear = youngestAgeAd
                                    CollectionDate = colDate
                                    Taxon = taxon
                                })
                            |> Dendro
                        )
                    else
                        let! depthsThisCore = getDepths ()
                        depthsThisCore |> Result.map(fun depths ->
                            Simple
                                {
                                    SiteName = siteName
                                    Origin = sampleOrigin depths.Depths
                                    Location = location
                                    OldestAge = oldestAge
                                    YoungestAge = youngestAge
                                    CoreDepthCm = coreDepth
                                    Proxies = proxyCategories |> Array.toList
                                }
                        )
            }

        ) |> Seq.toList


module Equality =

    let datesEqual (simple:OldDate.OldDateSimple) (com:OldDate.OldDatingMethod) =
        match com with
        | OldDate.OldDatingMethod.RadiocarbonCalibrated c ->
            match simple with
            | OldDate.OldDateSimple.CalYrBP (c2,_) -> c2 = c.CalibratedDate
            | _ -> false
        | OldDate.OldDatingMethod.RadiocarbonCalibratedRanges c -> false // TODO?
        | OldDate.OldDatingMethod.CollectionDate c ->
            match simple with
            | OldDate.OldDateSimple.HistoryYearAD c2 -> c2 = c
            | _ -> false
        | OldDate.OldDatingMethod.RadiocarbonUncalibrated u ->
            match simple with
            | OldDate.OldDateSimple.BP bp -> bp = u
            | _ -> false

/// Compile scenarios from the extracted data tables
let scenarios coreLocations dates : (Scenarios.Scenario * IndividualDateNode list) list =
    coreLocations
    |> List.collect(fun r ->
        match r with
        | Dendro d -> d |> List.map(fun d -> WoodRing d, [])
        | Simple loc ->

            let thisDates =
                dates
                |> List.choose id
                |> List.filter(fun d -> fst d = loc.SiteName.Value)

            let earliestUncertainty =
                thisDates
                |> List.tryFind(fun (_,d) -> Equality.datesEqual loc.OldestAge d.Date)
                |> Option.map snd
                |> Option.map(fun f -> f.MeasurementError)
                |> Option.defaultValue OldDate.MeasurementError.NoDatingErrorSpecified

            let latestUncertainty =
                thisDates
                |> List.tryFind(fun (_,d) -> Equality.datesEqual loc.YoungestAge d.Date)
                |> Option.map snd
                |> Option.map(fun f -> f.MeasurementError)
                |> Option.defaultValue OldDate.MeasurementError.NoDatingErrorSpecified

            [(Scenario.SiteOnlyEntry {
                SiteName = loc.SiteName
                SamplingLocation = loc.Location
                SampleOrigin = loc.Origin
                SampleLocationDescription = None
                EarliestYear = loc.OldestAge
                EarliestYearUncertainty = earliestUncertainty
                LatestYear = loc.YoungestAge
                LatestYearUncertainty = latestUncertainty
                Timeline = IndividualTimelineNode.Continuous TemporalResolution.Irregular // TODO Assumes this atm
                ProxyCategories = loc.Proxies
            }, thisDates |> List.map snd)]
        )
