// Script to automate connecting a microfossil diagram
// to it's 'real taxa' using lookup-based atlases / keys
// that exist in the graph database.

#load "nomenclatures.fsx"
open BiodiversityCoder.Core

/// ----
/// User-configurable options
/// ----
module Options =

    /// The timeline for which data should be added
    let timeline = System.Guid "9a793e68-13ff-4595-b5aa-5ff8905e44a5" // Bugt Sø

    /// Stops processing if some morphotypes are not matched
    /// within the specified key.
    let runWithIncompleteMatches = true

    /// Adds unmatched taxa with a placeholder to the 'Problematic' kingdom if true.
    let addUnmatchedTaxa = true

    /// If there are more than one raw dataset for the timeline,
    /// specify the raw data to use here.
    let specifiedRawDataset = None

    let proxyGroup = Population.BioticProxies.MicrofossilGroup.Pollen

    let outcome = Outcomes.Biodiversity.Abundance

    /// Specify an atlas or key to use by ID. If a lookup
    /// is available it will be used to make the proxied taxa.
    /// For a list of atlases, work from newest to oldest finding
    /// the first match for a morphotype. For pollen lookup, this will
    /// just be the one inference node.
    let inferenceAtlases = [ // List newest to oldest
        "atlas_lookup_fshsavhitssaegbggu"
        // "atlas_lookup_fbsitvhogpioshlabdmog"
    ]

    /// If a morphotype has been used with a different interchangable name for an
    /// identical morphotype, enter the names here.
    /// Key name -> this data name
    let morphotypeSynonyms = Map.ofList [
        "Saxifraga caespitosa type", "Saxifraga ceruna type"
    ]

    /// If botanical naming scheme has been defined, state it here.
    /// e.g. Bocher (1965) Flora of Greenland.
    let inferenceNomenclature = Some Nomenclatures.Floras.``Flora of Greenland (Böcher et al., 1968)``

    // /// If a specific convention on naming morphotypes has been applied,
    // /// name that here or use None for unspecified.
    // /// e.g. Faegri and Iversen (1975) as system for naming types.
    // let inferenceMorphotypeConvention = Some <| Graph.FriendlyKey("inferencemethodnode", "morphotypeterminology_fitopa")

    let dataFolder = "/Users/andrewmartin/Documents/GitHub Projects/holocene-arctic-biodiversity-map/data/"

    /// A manual lookup table to convert from pollen morphotypes into
    /// taxonomic entities (including full naming / nomenclature).
    let manualLookup = []

    /// Try to match keys and without sp. on the end for genera.
    let dataIsMissingSpSuffix = true


// ---------
// SCRIPT FUNCTIONS START
// ---------

// - Read in all of the individual proxy morphotypes from the data table.
// - Read the specified lookup table

module WorldFloraOnline =

    open FSharp.Data

    type WFO = JsonProvider<"wfo-sample.json", SampleIsList = true>        

    let query taxon =
        let latinName =
            match taxon with
            | Population.Taxonomy.Family f -> f.Value
            | Population.Taxonomy.Genus g -> g.Value
            | Population.Taxonomy.Species (g,s,a) -> sprintf "%s %s %s" g.Value s.Value a.Value
            | Population.Taxonomy.Subspecies (g,s, ss,a) -> sprintf "%s %s ssp. %s %s" g.Value s.Value ss.Value a.Value
            | _ -> "Unknown"
        sprintf "https://list.worldfloraonline.org/matching_rest.php?input_string=%s" latinName

    let tryMatch taxon (query:string) =
        let result = WFO.Load query
        // printfn "Result %A" result.Candidates
        result.Match
        |> Option.bind(fun m ->
            let tree = m.Placement.Split("/") |> Array.rev
            match taxon with
            | Population.Taxonomy.Species _ ->
                let regexMatch = System.Text.RegularExpressions.Regex.Match(m.FullNameHtml, "<span class=\"wfo-name-full\" ><span class=\"wfo-name\"><i>(.*)<\/i> <i>(.*)<\/i><\/span> <span class=\"wfo-name-authors\" >(.*)<\/span><\/span>")
                if tree.[2].EndsWith "eae"
                then
                    Some (Population.Taxonomy.Species(
                        regexMatch.Groups.[1].Value |> FieldDataTypes.Text.createShort |> forceOk,
                        regexMatch.Groups.[2].Value |> FieldDataTypes.Text.createShort |> forceOk,
                        regexMatch.Groups.[3].Value |> FieldDataTypes.Text.createShort |> forceOk),
                        [
                            Population.Taxonomy.Genus(tree.[1] |> FieldDataTypes.Text.createShort |> forceOk)
                            Population.Taxonomy.Family(tree.[2] |> FieldDataTypes.Text.createShort |> forceOk)
                            Population.Taxonomy.Kingdom("Plantae" |> FieldDataTypes.Text.createShort |> forceOk)
                        ])
                else
                    printfn "Warning: taxonomic tree appeared corrupted / non-standard for %s (%s). Not using." m.FullNamePlain m.Placement
                    None
            | Population.Taxonomy.Genus _ ->
                let regexMatch = System.Text.RegularExpressions.Regex.Match(m.FullNameHtml, "<span class=\"wfo-name-full\" ><span class=\"wfo-name\"><i>(.*)<\/i><\/span> <span class=\"wfo-name-authors\" >(.*)<\/span><\/span>")
                if tree.[1].EndsWith "aceae"
                then
                    Some (Population.Taxonomy.Genus(regexMatch.Groups.[1].Value |> FieldDataTypes.Text.createShort |> forceOk),
                    [
                        Population.Taxonomy.Family(tree.[1] |> FieldDataTypes.Text.createShort |> forceOk)
                        Population.Taxonomy.Kingdom("Plantae" |> FieldDataTypes.Text.createShort |> forceOk)
                    ])
                else
                    printfn "Warning: taxonomic tree appeared corrupted / non-standaed for %s (%s). Not using." m.FullNamePlain m.Placement
                    None
            | Population.Taxonomy.Family _ ->
                let regexMatch = System.Text.RegularExpressions.Regex.Match(m.FullNameHtml, "<span class=\"wfo-name-full\" ><span class=\"wfo-name\">(.*)<\/span> <span class=\"wfo-name-authors\" >(.*)<\/span><\/span>")
                Some (Population.Taxonomy.Family(regexMatch.Groups.[1].Value |> FieldDataTypes.Text.createShort |> forceOk),
                [ Population.Taxonomy.Kingdom("Plantae" |> FieldDataTypes.Text.createShort |> forceOk) ])
            | _ -> None )

let fetchAtlas inferenceAtlas graph =
    Storage.atomByKey (Graph.FriendlyKey("inferencemethodnode",inferenceAtlas)) graph
    |> Result.ofOption "Could not find atlas node"
    |> Result.bind (fun a ->
        match (a |> fst |> snd) with
        | GraphStructure.Node.PopulationNode p ->
            match p with
            | GraphStructure.InferenceMethodNode i ->
                match i with
                | Population.BioticProxies.InferenceMethodNode.IdentificationKeyOrAtlasWithLookup l -> Ok (i, Some l)
                | _ -> Ok (i, None)
            | _ -> Error "Not an atlas node"
        | _ -> Error "Not an atlas node" )

let stackAtlases (atlases: list<Population.BioticProxies.InferenceMethodNode * option<Population.BioticProxies.TaxonLookup>>) =
    atlases 
    |> List.choose(fun (n,l) -> l |> Option.map(fun l -> l.Entries |> List.map(fun e -> e,n)))
    |> List.collect id


let run () =
    result {

        let! graph = Storage.loadOrInitGraph Options.dataFolder

        let! timeline =
            Storage.atomByKey (Graph.UUID("individualtimelinenode", Options.timeline)) graph
            |> Result.ofOption "Could not find timeline node"

        let! measureNode = Storage.atomByKey (GraphStructure.makeUniqueKey (GraphStructure.OutcomeNode <| GraphStructure.MeasureNode Options.outcome)) graph |> Result.ofOption ""

        let! stackedAtlas =
            Options.inferenceAtlases
            |> List.map(fun a -> fetchAtlas a graph)
            |> Result.ofList
            |> Result.map stackAtlases

        let! rawData =
            timeline |> snd
            |> Seq.tryPick(fun (_,sink,_,r) ->
                match r with
                | GraphStructure.Relation.Exposure e ->
                    match e with
                    | Exposure.ExposureRelation.HasRawData -> Some sink
                    | _ -> None
                | _ -> None )
            |> Option.bind(fun dId -> Storage.atomByKey dId graph )
            |> Result.ofOption "Could not find raw dataset"
            |> Result.bind(fun atom ->
                match atom |> fst |> snd with
                | GraphStructure.Node.DatasetNode d ->
                    match d with
                    | Datasets.DatasetNode.Digitised d2 -> Ok d2
                | _ -> Error "Not a dataset node" )

        let dataMorphotypes =
            match Options.proxyGroup with
            | Population.BioticProxies.PlantMacrofossil ->
                rawData.DataTable.Morphotypes ()
                |> List.map(fun morphotype -> System.Text.RegularExpressions.Regex.Replace(morphotype, " \(.*\)$", ""))
            | _ -> rawData.DataTable.Morphotypes ()

            // |> List.map(fun x -> Options.morphotypeSynonyms |> Map.tryFind x |> Option.defaultValue x)
        do!
            let inKey =
                match Seq.isEmpty stackedAtlas with
                | false -> stackedAtlas |> List.map(fun (a,b) -> a.MorphotypeName.Value)
                | true -> Options.manualLookup |> List.map fst
                // Also match in flora if specified:
                |> List.append (Options.inferenceNomenclature |> Option.map(fun l -> fst l |> List.map fst) |> Option.defaultValue [] )
                |> List.map(fun s -> if Options.dataIsMissingSpSuffix then s.Replace(" sp.", "") else s)
                |> List.map(fun s ->
                    match Options.morphotypeSynonyms |> Map.tryFind s with
                    | Some m ->
                        printfn "[Info] Replacing morphotype %s with synonym %s" s m
                        m
                    | None -> s )

            let intersect = Set.intersect (Set.ofList dataMorphotypes) (Set.ofList inKey)
            printfn "Found %i / %i morphotypes in the atlas" intersect.Count dataMorphotypes.Length
            if intersect.Count <> dataMorphotypes.Length && not Options.runWithIncompleteMatches
            then Error (sprintf "%i morphotypes are not in the atlas: %A" (dataMorphotypes.Length - intersect.Count) (List.except (Set.toList intersect) dataMorphotypes))
            else
                List.except (Set.toList intersect) dataMorphotypes
                |> List.iter(fun m -> printfn "[Warning] '%s' not in key / atlas." m)
                Ok ()

        // Only assign morphotypes not already added as proxied taxa.
        let! proxiesAlreadyEntered =
            timeline |> snd
            |> List.choose(fun (_,sink,_,r) ->
                match r with
                | GraphStructure.Relation.Exposure e ->
                    match e with
                    | Exposure.ExposureRelation.HasProxyInfo -> Some sink
                    | _ -> None
                | _ -> None )
            |> Storage.loadAtoms graph.Directory "PopulationNode"
            |> Result.bind(fun r ->
                r |> List.map(fun r ->
                    r |> snd |> Seq.pick(fun (_,sink,_,r) ->
                    match r with
                    | GraphStructure.Relation.Population e ->
                        match e with
                        | Population.PopulationRelation.InferredFrom -> Some sink
                        | _ -> None
                    | _ -> None )
                )
                |> Storage.loadAtoms graph.Directory "PopulationNode"
                |> Result.map(fun r ->
                    r |> List.choose(fun r ->
                        match r |> fst |> snd with
                        | GraphStructure.Node.PopulationNode p ->
                            match p with
                            | GraphStructure.BioticProxyNode p -> Some p
                            | _ -> None
                        | _ -> None
                        )
                    )
            )
        
        printfn "The following proxies are already entered: %A" proxiesAlreadyEntered

        let! nomenclatureForLookup =
            Options.inferenceNomenclature 
            |> Option.map(fun (key,nomId) ->
                Storage.atomByKey nomId graph
                |> Result.ofOption "Could not find inference node (nomenclature)"
                |> Result.bind(fun inferAtom ->
                    match inferAtom |> fst |> snd with
                    | GraphStructure.PopulationNode e ->
                        match e with
                        | GraphStructure.InferenceMethodNode i -> Ok i
                        | _ -> Error "Not an inference node"
                    | _ -> Error "Not an inference node" )
                |> Result.map(fun inferNode ->
                    key 
                    |> List.map(fun (name,taxa) -> name, inferNode, taxa)))
            |> Option.defaultValue (Ok [])

        // Lookup ordered with morphotypes first, and nomenclature second.
        let lookupToUse =
            List.append (
                match Seq.isEmpty stackedAtlas with
                | false -> stackedAtlas |> List.map(fun (a,b) -> a.MorphotypeName.Value, b, a.Taxa)
                | true -> Options.manualLookup) nomenclatureForLookup

        printfn "Lookup = ..."
        lookupToUse |> List.iter(fun i -> printfn "%A" i)
        System.Console.ReadLine() |> ignore


        let proxiedTaxa =
            dataMorphotypes
            |> List.map(fun m ->
                lookupToUse 
                |> List.tryFind(fun (a,_,_) ->
                    if Options.dataIsMissingSpSuffix
                    then
                        printfn "[%s] [%s]" (a.Replace(" sp.", "")) m
                        a = m || a.Replace(" sp.", "") = m
                    else a = m )
                |> Option.map(fun (a,b,c) -> if Options.dataIsMissingSpSuffix then a.Replace(" sp.", ""),b,c else a,b,c)
                |> Option.defaultWith(fun _ ->
                    printfn "[Info] Using placeholder taxon for morphotype '%s'." m
                    (m, Population.BioticProxies.Implicit, [ Population.Taxonomy.Kingdom (forceOk <| FieldDataTypes.Text.createShort "Problematic (Placeholder)")])
                )
            )
            |> List.map(fun (entry, inferNode, taxa) ->
                let morphotypeNode =
                    let name =
                        match Options.proxyGroup with
                        | Population.BioticProxies.PlantMacrofossil -> entry.Replace(" (any)", "")
                        | _ -> entry
                    Population.BioticProxies.Microfossil (Options.proxyGroup, FieldDataTypes.Text.createShort name |> forceOk)
                    |> Population.BioticProxies.BioticProxyNode.Morphotype
                morphotypeNode, inferNode, taxa
            )
            |> List.filter(fun (m,_,_) -> proxiesAlreadyEntered |> List.contains m |> not)

        printfn "%i proxies remain to be entered" proxiedTaxa.Length

        let taxa =
            proxiedTaxa
            |> List.collect(fun (_,_,t) -> t)
            |> List.distinct
            |> List.choose(fun t ->
                WorldFloraOnline.query t 
                |> WorldFloraOnline.tryMatch t
                |> Option.orElseWith(fun _ ->
                    printfn "[Warning] Not a WFO match for %A." t
                    if Options.addUnmatchedTaxa
                    then  Some (t, [ Population.Taxonomy.Kingdom (forceOk <| FieldDataTypes.Text.createShort "Plantae")])
                    else None ) )

        let taxonToNode taxon =
            taxon |> GraphStructure.TaxonomyNode |> GraphStructure.PopulationNode

        printfn "Taxa are %A" taxa

        printfn "Adding %i taxa into graph database (skips if already present)..." taxa.Length

        let! graphWithTaxa =
            taxa
            |> List.collect(fun (taxon, heirarchy) ->
                taxon :: heirarchy |> List.map taxonToNode |> List.windowed 2 |> List.map(fun l -> l.[0], l.[1]))
            |> List.rev // Assume parents already added
            |> List.fold(fun state (child, parent) -> 
                    state
                    |> Result.bind(fun s ->
                        let existing = Storage.atomByKey (GraphStructure.makeUniqueKey child) s
                        if existing.IsNone
                        then
                            printfn "Adding %A -> %A" child parent
                            Storage.addNodes s [ child ]
                            |> Result.bind(fun (g,a) ->
                                Storage.addRelationByKey g
                                    (GraphStructure.makeUniqueKey child)
                                    (GraphStructure.makeUniqueKey parent)
                                    (GraphStructure.ProposedRelation.Population (Population.PopulationRelation.IsA))
                                )
                        else
                            printfn "Already exists: %A" child
                            state
                    )
            ) (Ok graph)

        printfn "Adding proxy hyperedges into graph database..."
        printfn "(adding nodes for biotic proxies if they don't already exist)"

        let! graphWithProxied =
            List.fold(fun state (morpho, inferNode,taxa) -> 
                state 
                |> Result.bind(fun s -> Storage.addOrSkipNodes s [ GraphStructure.PopulationNode <| GraphStructure.BioticProxyNode morpho ])
                |> Result.bind(fun (s,_) ->
                    printfn "Taxa are %A. %A" (List.head taxa) (List.tail taxa)
                    Storage.addProxiedTaxon
                        morpho
                        (taxa |> List.head)
                        (taxa |> List.tail)
                        inferNode
                        ([] |> List.choose id)
                        s
                    |> Result.bind(fun (g, proxiedKey) -> 
                        Storage.addRelationByKey g (timeline |> fst |> fst) proxiedKey (GraphStructure.ProposedRelation.Exposure Exposure.ExposureRelation.HasProxyInfo)
                        |> Result.lift(fun r -> r, proxiedKey))
                    |> Result.bind(fun (g, proxiedKey) -> 
                        Storage.addRelationByKey g proxiedKey (measureNode |> fst |> fst) (GraphStructure.ProposedRelation.Population <| Population.PopulationRelation.MeasuredBy) )
                    )
            ) (Ok graphWithTaxa) proxiedTaxa

        return graphWithProxied
    }

run ()
