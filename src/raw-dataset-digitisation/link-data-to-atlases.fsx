// Script to automate connecting a pollen diagram
// to it's 'real taxa' using lookup-based atlases / keys
// that exist in the graph database.

#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core

/// ----
/// User-configurable options
/// ----
module Options =

    /// The timeline for which data should be added
    let timeline = System.Guid "c2805b46-f963-491a-b825-766208e247a0"

    /// Stops processing if some morphotypes are not matched
    /// within the specified key.
    let runWithIncompleteMatches = true

    /// If there are more than one raw dataset for the timeline,
    /// specify the raw data to use here.
    let specifiedRawDataset = None

    let proxyGroup = Population.BioticProxies.Pollen

    let outcome = Outcomes.Biodiversity.Abundance

    let atlasToUse = Graph.FriendlyKey("inferencemethodnode", "atlas_lookup_fbsitvhogpioshlabdmog")

    let dataFolder = "../../data/"


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
        result.Match
        |> Option.bind(fun m ->
            let tree = m.Placement.Split("/") |> Array.rev
            match taxon with
            | Population.Taxonomy.Species _ ->
                let regexMatch = System.Text.RegularExpressions.Regex.Match(m.FullNameHtml, "<span class=\"wfo-name-full\" ><span class=\"wfo-name\"><i>(.*)<\/i> <i>(.*)<\/i><\/span> <span class=\"wfo-name-authors\" >(.*)<\/span><\/span>")
                if tree.[2].EndsWith "aceae"
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
                    printfn "Warning: taxonomic tree appeared corrupted / non-standaed for %s (%s). Not using." m.FullNamePlain m.Placement
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

        let! atlas =
            Storage.atomByKey Options.atlasToUse graph
            |> Result.ofOption "Could not find atlas node"
            |> Result.bind (fun a ->
                match (a |> fst |> snd) with
                | GraphStructure.Node.PopulationNode p ->
                    match p with
                    | GraphStructure.InferenceMethodNode i ->
                        match i with
                        | Population.BioticProxies.InferenceMethodNode.IdentificationKeyOrAtlasWithLookup l -> Ok l
                        | _ -> Error "Not a lookup-based atlas"
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
            rawData.DataTable.Morphotypes ()
            // |> List.map(fun x -> Options.morphotypeSynonyms |> Map.tryFind x |> Option.defaultValue x)
        do!
            let intersect = Set.intersect (Set.ofList dataMorphotypes) (Set.ofList (atlas.Entries |> List.map(fun a -> a.MorphotypeName.Value)))
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

        let proxiedTaxa =
            dataMorphotypes
            |> List.filter(fun m -> atlas.Entries |> List.exists(fun a -> a.MorphotypeName.Value = m))
            |> List.map(fun morphotype ->
                let entry =
                    atlas.Entries
                    |> List.find(fun e -> e.MorphotypeName.Value = morphotype)
                let morphotypeNode =
                    Population.BioticProxies.Microfossil (Options.proxyGroup, entry.MorphotypeName)
                    |> Population.BioticProxies.BioticProxyNode.Morphotype
                morphotypeNode, atlas, entry.Taxa
            )
            |> List.filter(fun (m,_,_) -> proxiesAlreadyEntered |> List.contains m |> not)

        printfn "%i proxies remain to be entered" proxiedTaxa.Length

        let taxa =
            proxiedTaxa
            |> List.collect(fun (_,_,t) -> t)
            |> List.choose(fun t -> WorldFloraOnline.query t |> WorldFloraOnline.tryMatch t )

        let taxonToNode taxon =
            taxon |> GraphStructure.TaxonomyNode |> GraphStructure.PopulationNode

        printfn "Adding %i taxa into graph database (skips if already present)..." taxa.Length

        let! graphWithTaxa =
            List.fold(fun state (taxon, heirarchy) -> 
                state
                |> Result.bind(fun s ->
                    Storage.addOrSkipNodes s (taxon :: heirarchy |> List.map taxonToNode)
                    |> Result.map fst)
            ) (Ok graph) taxa

        printfn "Adding proxy hyperedges into graph database..."
        printfn "(adding nodes for biotic proxies if they don't already exist)"

        let! graphWithProxied =
            List.fold(fun state (morpho,infer,taxa) -> 
                state 
                |> Result.bind(fun s -> Storage.addOrSkipNodes s [ GraphStructure.PopulationNode <| GraphStructure.BioticProxyNode morpho ])
                |> Result.bind(fun (s,_) ->
                    Storage.addProxiedTaxon
                        morpho
                        (taxa |> List.head)
                        (taxa |> List.tail)
                        (Population.BioticProxies.InferenceMethodNode.IdentificationKeyOrAtlasWithLookup infer)
                        s
                    |> Result.bind(fun (g, proxiedKey) -> 
                        Storage.addRelationByKey g (timeline |> fst |> fst) proxiedKey (GraphStructure.ProposedRelation.Exposure Exposure.ExposureRelation.HasProxyInfo)
                        |> Result.lift(fun r -> r, proxiedKey))
                    |> Result.bind(fun (g, proxiedKey) -> 
                        Storage.addRelationByKey g proxiedKey (measureNode |> fst |> fst) (GraphStructure.ProposedRelation.Population <| Population.PopulationRelation.MeasuredBy) )
                    )
            ) (Ok graphWithTaxa) proxiedTaxa

        return! Ok graphWithProxied
    }

run ()
