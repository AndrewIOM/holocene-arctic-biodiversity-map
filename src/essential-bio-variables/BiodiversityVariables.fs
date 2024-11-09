module BiodiversityVariables

open BiodiversityCoder.Core
open BiodiversityCoder.Core.FieldDataTypes

// What are the biodiversity variables of interest?
// Per-taxon probability of occurrence per 500-year interval

/// Common time-bins to use across EBVs.
let timeBins = [ 0<OldDate.calYearBP> .. 500<OldDate.calYearBP> .. 12500<OldDate.calYearBP> ]

/// Identify time-series that have raw datasets
/// associated with them.
let rawDataByTimeSeriesId graph =
    result {
        let! timelines = DateHarmonisation.readAllTimelineAtoms graph

        let withRawData =
            timelines
            |> List.collect(fun (a:Graph.Atom<GraphStructure.Node, GraphStructure.Relation>) -> 
                (snd a)
                |> List.choose(fun (_,sinkId,_,conn) ->
                    match conn with
                    | GraphStructure.Relation.Exposure r ->
                        match r with
                        | Exposure.ExposureRelation.HasRawData ->
                            Some (a, sinkId)
                        | _ -> None
                    | _ -> None ))
            |> List.choose(fun (a,rdId) ->
                Storage.loadAtom graph.Directory (typeof<Exposure.ExposureNode>.Name) rdId
                |> Result.toOption
                |> Option.bind(fun rawNode ->
                    match rawNode |> fst |> snd with
                    | GraphStructure.Node.DatasetNode d ->
                        match d with
                        | Datasets.DatasetNode.Digitised c -> Some (a ,c)
                    | _ -> None ))

        printfn "With raw %A" withRawData

        // Get individual dates.
        // Follow them to the calibration node.
        // Assume age-depth model based for now.
        let withCalibrations =
            withRawData
            |> List.choose(fun a ->
                
                let dates =
                    a |> fst |> snd
                    |> List.choose(fun (_,sinkId,_,conn) ->
                        match conn with
                        | GraphStructure.Relation.Exposure r ->
                            match r with
                            | Exposure.ExposureRelation.ConstructedWithDate ->
                                Some sinkId
                            | _ -> None
                        | _ -> None )
                
                dates
                |> Storage.loadAtoms graph.Directory (typeof<Exposure.ExposureNode>.Name)
                |> Result.toOption
                |> Option.bind(fun dateNodes ->
                
                    let calibrations =
                        dateNodes
                        |> List.collect(fun (_,rels) ->
                            rels
                            |> List.choose(fun (_,sinkId,_,conn) ->
                            match conn with
                            | GraphStructure.Relation.Exposure r ->
                                match r with
                                | Exposure.ExposureRelation.UsedInCalibration ->
                                    Some sinkId
                                | _ -> None
                            | _ -> None ))
                        |> List.distinct

                    match calibrations.Length with
                    | 0 -> None
                    | 1 ->
                        Storage.loadAtom graph.Directory (typeof<Exposure.ExposureNode>.Name) calibrations.Head
                        |> Result.toOption
                        |> Option.bind(fun n ->
                            match n |> fst |> snd with
                            | GraphStructure.Node.ExposureNode e ->
                                match e with
                                | Exposure.ExposureNode.DateCalibrationInstanceNode c -> Some (fst a |> fst |> fst, snd a, c)
                                | _ -> None
                            | _ -> None
                            )
                    | _ ->
                        printfn "Multiple calibrations existed for a timeline. Skipping."
                        None
                    
                    )
            )

        printfn "With cal %A" withCalibrations

        return withCalibrations
    }

/// Calculate the per-taxon probability of occurrence
/// per time-bin. 
/// - Initially work with genera.
/// - Collapse data to presence-only.
/// - Taxonomic uncertainty - work off 'real' taxa rather than morphotypes.
/// - Temporal uncertainty - assume intersection of calibrated bound 
let perTaxonProbabilityOfOccurrence =

    // Likelihood of presence within a time-period is:
    // - probability of date overlap with time-period.
    // - 

    ()

/// Lookup the age of the depth level in an age-depth
/// model, if one is provided.
let applyAgeDepthModelToData (dataset: Datasets.DigitisedDataset) (cal:Exposure.Reanalysis.DateCalibrationNode) =

    cal.AgeDepthModel
    |> Option.bind(fun ageDepth ->

        let ageDepth =
            ageDepth
            |> List.sortBy(fun d -> d.Depth)
        
        let morphotypes = dataset.DataTable.Morphotypes()

        let indexType, data = dataset.DataTable.Depths()
        match indexType with
        | Datasets.DataTable.IndexUnit.Depths ->
           
           data
           |> Seq.choose(fun kv ->
                
                let nearestDepth = ageDepth |> Seq.tryFind(fun d -> d.Depth >= (kv.Key * 1.<StratigraphicSequence.cm>))
                nearestDepth
                |> Option.map(fun newDepth -> newDepth, kv.Value |> Seq.zip morphotypes |> Map.ofSeq)
            )
            |> Map.ofSeq
            |> Some

        | Datasets.DataTable.IndexUnit.Ages unit ->
            printfn "Skipping age-indexed dataset, as no lookup function configured."
            None
    )

open Population.BioticProxies

let toMorphotypeName (node: Population.BioticProxies.BioticProxyNode) =
    match node with
    | BioticProxyNode.AncientDNA a -> a.Value
    | BioticProxyNode.Morphotype m ->
        match m with
        | Morphotype.Microfossil (_,n) -> n.Value
        | Morphotype.Macrofossil (_,n) -> n.Value
        | Morphotype.Megafossil(_, morphotypeName) -> morphotypeName.Value
    | BioticProxyNode.ContemporaneousWholeOrganism(taxon) -> taxon.Value


/// Given the text representation of the morphotypes
/// in the dataframe, link this to the morphotypes used
/// in the biotic proxy nodes. Then, follow to the 'real'
/// taxa identities and attach to the result.
let proxiedTaxaLookup timelineId (graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation>) =
    result {
        let! timeline = Storage.loadAtom graph.Directory (typeof<Exposure.ExposureNode>.Name) timelineId
        
        let! proxyHyperedges =
            timeline |> snd
            |> List.choose(fun (_,sinkId,_,conn) ->
                match conn with
                | GraphStructure.Relation.Exposure r ->
                    match r with
                    | Exposure.ExposureRelation.HasProxyInfo ->
                        Some sinkId
                    | _ -> None
                | _ -> None )
            |> Storage.loadAtoms graph.Directory (typeof<GraphStructure.PopulationNode>.Name)
        
        // For each hyperedge, figure out its morphotype <-> real taxon
        let taxonLookup =
            proxyHyperedges
            |> List.choose(fun (_,rels) ->

                let morphotype =
                    rels |> List.tryPick(fun (_,sinkId,_,conn) ->
                        match conn with
                        | GraphStructure.Relation.Population r ->
                            match r with
                            | Population.PopulationRelation.InferredFrom ->
                                Some sinkId
                            | _ -> None
                        | _ -> None
                    )
                    |> Option.bind (fun x -> x |> Storage.loadAtom graph.Directory (typeof<GraphStructure.PopulationNode>.Name) |> Result.toOption)
                    |> Option.bind(fun atom ->
                        match atom |> fst |> snd with
                        | GraphStructure.Node.PopulationNode p ->
                            match p with
                            | GraphStructure.BioticProxyNode b -> Some b
                            | _ -> None
                        | _ -> None
                        )

                let taxa =
                    rels |> List.choose(fun (_,sinkId,_,conn) ->
                        match conn with
                        | GraphStructure.Relation.Population r ->
                            match r with
                            | Population.PopulationRelation.InferredAs ->
                                Some sinkId
                            | _ -> None
                        | _ -> None
                    )
                    |> Storage.loadAtoms graph.Directory (typeof<GraphStructure.PopulationNode>.Name)
                    |> Result.defaultValue []
                    |> List.choose(fun atom ->
                            match atom |> fst |> snd with
                            | GraphStructure.Node.PopulationNode p ->
                                match p with
                                | GraphStructure.TaxonomyNode b -> Some b
                                | _ -> None
                            | _ -> None )

                match morphotype, taxa with
                | Some m, t when t.Length > 0 -> Some (toMorphotypeName m, t)
                | _ -> None
            )

        return taxonLookup |> Map.ofList
    }
    

let applyRealTaxaToDataMorphotypes (lookup:Map<string,Population.Taxonomy.TaxonNode>) (morphotypes:string list) =
    morphotypes |> List.map(fun name -> Map.tryFind name lookup )


let calculateBiodiversityVariables graph =
    result {

        printfn "1"

        let! series = rawDataByTimeSeriesId graph

        printfn "%A" series

        let newAges =
            series |> List.choose(fun (timeline, data, calibration ) ->
                applyAgeDepthModelToData data calibration
                |> Option.map(fun r ->
                    let taxonLookup = proxiedTaxaLookup timeline graph |> Result.forceOk
                    timeline, r, taxonLookup)
            )

        printfn "3"

        return newAges

    }
