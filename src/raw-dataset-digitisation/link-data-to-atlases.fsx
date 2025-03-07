// Script to automate connecting a microfossil diagram
// to it's 'real taxa' using lookup-based atlases / keys
// that exist in the graph database.

#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core
open Population.Taxonomy

module ManualLookups =

    /// 1968 edition of the Flora of Greenland. Lookup of macrofossil names as mentioned
    /// in various Fredskild 1970s - 1980s macrofossil diagrams.
    let floraOfGreenland =
        let txt s = s |> FieldDataTypes.Text.createShort |> forceOk
        [
            "Alchemilla alpina", [ Species(txt "Alchemilla", txt "alpina", txt "L.") ]
            "Betula nana", [ Species(txt "Betula", txt "nana", txt "L.") ]
            // "Carex arctogena", []
            "Carex bigelowii", [ Species(txt "Carex", txt "bigelowii", txt "Torr.") ]
            "Carex brunescens", [ Species(txt "Carex", txt "brunescens", txt "(Pers.) Poir.") ]
            "Carex capillaris", [ Species(txt "Carex", txt "capillaris", txt "L.") ]
            "Carex gynocrates", [ Species(txt "Carex", txt "gynocrates", txt "Wormsk.") ]
            "Carex lachenalii", [ Species(txt "Carex", txt "lachenalii", txt "Schkuhr.") ]
            "Carex norvegica", [ Species(txt "Carex", txt "norvegica", txt "L.") ]
            "Carex rufina", [ Species(txt "Carex", txt "rufina", txt "Drej.") ]
            // "Carex sect. heleonastes", [ Species(txt "", txt "", txt "") ]
            "Carex sp. distigmitat", [ Genus(txt "Carex") ]
            "Carex sp. tristigmat.", [ Genus(txt "Carex") ]
            "Chamaenerion latifolium", [ Species(txt "Chamaenerion", txt "latifolium", txt "(L.) Th. Fries & Lge.") ]
            "Draba sp.", [ Genus(txt "Draba") ]
            "Dryas integrifolia", [ Species(txt "Dryas", txt "integrifolia", txt "M. Vahl.") ]
            "Empetrum hermaphroditum", [ Species(txt "Empetrum", txt "hermaphroditum", txt "(Hagerup) Böch.") ]
            "Ericales sp. and Salix herbacea", [
                Order(txt "Ericales")
                Species(txt "Salix", txt "herbacea", txt "L.") ]
            "Harrimanella hypnoides", [ Species(txt "Harrimanella", txt "hypnoides", txt "(L.) Coville.") ]
            "Hippuris vulgaris", [ Species(txt "Hippuris", txt "vulgaris", txt "L.") ]
            "Juncus bi- and tri-triglumis", [ Genus(txt "Juncus") ]
            "Juniperus communis", [ Species(txt "Juniperus", txt "communis", txt "L.") ]
            // "Loiseleuria decumbens", [ Species(txt "", txt "", txt "") ]
            "Luzula sp.", [ Genus(txt "Luzula") ]
            "Minuratia rubella", [ Species(txt "Minuratia", txt "rubella", txt "(Wbg.) Hiern.") ]
            "Phippsia algida", [ Species(txt "Phippsia", txt "algida", txt "(Sol.) R. Br.") ]
            "Phyllodoce", [ Genus(txt "Phyllodoce") ]
            "Poa sp.", [ Genus(txt "Poa") ]
            "Potamogeton pusillus", [ Species(txt "Potamogeton", txt "pusillus", txt "L.") ]
            "Potentilla sp.", [ Genus(txt "Potentilla") ]
            "Ranunculus confervoides", [ Species(txt "Ranunculus", txt "confervoides", txt "(Fr.) Asch. & Graebn.") ]
            "Ranunculus hyperboreus", [ Species(txt "Ranunculus", txt "hyperboreus", txt "Rottb.") ]
            "Salix", [ Genus(txt "Salix") ]
            "Salix herbacea", [ Species(txt "Salix", txt "herbacea", txt "L.") ]
            "Silene acaulis", [ Species(txt "Silene", txt "acaulis", txt "(L.) Jacq.") ]
            "Vaccinium uliginosum", [ Species(txt "Vaccinium", txt "uliginosum", txt "L.") ]
        ]


/// ----
/// User-configurable options
/// ----
module Options =

    /// The timeline for which data should be added
    let timeline = System.Guid "48d66bcb-89ff-4d3f-8713-057de0eb592b"

    /// Stops processing if some morphotypes are not matched
    /// within the specified key.
    let runWithIncompleteMatches = true

    /// If there are more than one raw dataset for the timeline,
    /// specify the raw data to use here.
    let specifiedRawDataset = None

    let proxyGroup = Population.BioticProxies.MicrofossilGroup.PlantMacrofossil

    let outcome = Outcomes.Biodiversity.Abundance

    let atlasToUse = Graph.FriendlyKey("inferencemethodnode", "atlas_ghnhmd")

    let dataFolder = "/Users/andrewmartin/Documents/GitHub Projects/holocene-arctic-biodiversity-map/data/"

    /// If a in-built lookup is not used, specify one manually here:
    let manualLookup = ManualLookups.floraOfGreenland

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
        printfn "Result %A" result.Candidates
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


let run () =
    result {

        let! graph = Storage.loadOrInitGraph Options.dataFolder

        let! timeline =
            Storage.atomByKey (Graph.UUID("individualtimelinenode", Options.timeline)) graph
            |> Result.ofOption "Could not find timeline node"

        let! measureNode = Storage.atomByKey (GraphStructure.makeUniqueKey (GraphStructure.OutcomeNode <| GraphStructure.MeasureNode Options.outcome)) graph |> Result.ofOption ""

        let! inferNode, atlas =
            Storage.atomByKey Options.atlasToUse graph
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
                match atlas with
                | Some a -> a.Entries |> List.map(fun a -> a.MorphotypeName.Value)
                | None -> Options.manualLookup |> List.map fst
            let intersect = Set.intersect (Set.ofList dataMorphotypes) (Set.ofList inKey)
            printfn "Found %i / %i morphotypes in the atlas" intersect.Count dataMorphotypes.Length
            if intersect.Count <> dataMorphotypes.Length && not Options.runWithIncompleteMatches
            then Error (sprintf "%i morphotypes are not in the atlas: %A" (dataMorphotypes.Length - intersect.Count) (List.except (Set.toList intersect) dataMorphotypes))
            else Ok ()

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

        let lookupToUse =
            match atlas with
            | Some a -> a.Entries |> List.map(fun a -> a.MorphotypeName.Value, a.Taxa)
            | None -> Options.manualLookup

        let proxiedTaxa =
            dataMorphotypes
            |> List.filter(fun m -> lookupToUse |> List.exists(fun a -> fst a = m))
            |> List.map(fun morphotype ->
                let entry =
                    lookupToUse
                    |> List.find(fun e -> fst e = morphotype)
                let morphotypeNode =
                    let name =
                        match Options.proxyGroup with
                        | Population.BioticProxies.PlantMacrofossil -> (fst entry).Replace(" (any)", "")
                        | _ -> fst entry
                    Population.BioticProxies.Microfossil (Options.proxyGroup, FieldDataTypes.Text.createShort name |> forceOk)
                    |> Population.BioticProxies.BioticProxyNode.Morphotype
                morphotypeNode, snd entry
            )
            |> List.filter(fun (m,_) -> proxiesAlreadyEntered |> List.contains m |> not)

        printfn "%i proxies remain to be entered" proxiedTaxa.Length

        let taxa =
            proxiedTaxa
            |> List.collect(fun (_,t) -> t)
            // |> List.distinct
            // |> List.map(fun t ->
            //     WorldFloraOnline.query t |> WorldFloraOnline.tryMatch t
            //     |> Option.defaultWith(fun _ ->
            //         printfn "No match in WFO for %A" t
            //         t, [] ) )
            |> List.choose(fun t -> WorldFloraOnline.query t |> WorldFloraOnline.tryMatch t )

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
                                    (GraphStructure.makeUniqueKey child)
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
            List.fold(fun state (morpho,taxa) -> 
                state 
                |> Result.bind(fun s -> Storage.addOrSkipNodes s [ GraphStructure.PopulationNode <| GraphStructure.BioticProxyNode morpho ])
                |> Result.bind(fun (s,_) ->
                    printfn "Taxa are %A. %A" (List.head taxa) (List.tail taxa)
                    Storage.addProxiedTaxon
                        morpho
                        (taxa |> List.head)
                        (taxa |> List.tail)
                        inferNode
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
