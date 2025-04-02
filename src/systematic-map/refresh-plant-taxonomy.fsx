// Script to re-assess plant name heirarchy against
// latest release of World Flora Online.

#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core

/// ----
/// User-configurable options
/// ----
module Options =

    let dataFolder = "/Users/andrewmartin/Documents/GitHub Projects/holocene-arctic-biodiversity-map/data"

    let skipWfoLinkedTaxa = true
    

module WorldFloraOnline =

    open FSharp.Data

    type WFOMatchService = JsonProvider<"../raw-dataset-digitisation/wfo-sample.json", SampleIsList = true>        
    type WorldFloraStableUrl = XmlProvider<"wfo-stableurl-sample.xml", SampleIsList = true>

    type StableUrlCached() =
        let mutable data : Map<string, WorldFloraStableUrl.Rdf> = Map.empty
        member this.Load url =
            match data |> Map.tryFind url with
            | Some u -> u
            | None ->
                let newData = WorldFloraStableUrl.Load url
                data <- data |> Map.add url newData
                newData

    let stableCache = StableUrlCached()

    let matchQueryString taxon =
        let latinName =
            match taxon with
            | Population.Taxonomy.Family f -> f.Value
            | Population.Taxonomy.Genus g -> g.Value
            | Population.Taxonomy.Species (g,s,a) -> sprintf "%s %s %s" g.Value s.Value a.Value
            | Population.Taxonomy.Subspecies (g,s, ss,a) -> sprintf "%s %s ssp. %s %s" g.Value s.Value ss.Value a.Value
            | Population.Taxonomy.Variety (g,s, ss,a) -> sprintf "%s %s var. %s %s" g.Value s.Value ss.Value a.Value
            | Population.Taxonomy.Clade g -> g.Value
            | Population.Taxonomy.Class g -> g.Value
            | Population.Taxonomy.Tribe g -> g.Value
            | Population.Taxonomy.Kingdom g -> g.Value
            | Population.Taxonomy.Order g -> g.Value
            | Population.Taxonomy.Phylum g -> g.Value
            | Population.Taxonomy.Subfamily g -> g.Value
            | Population.Taxonomy.Subgenus g -> g.Value
            | Population.Taxonomy.Subtribe g -> g.Value
            | Population.Taxonomy.Life -> "Unknown"
            
        sprintf "https://list.worldfloraonline.org/matching_rest.php?input_string=%s" latinName


    let rec heirarchyFor' (parent: option<WorldFloraStableUrl.IsPartOf>) (name: WorldFloraStableUrl.TaxonName) tree =
        let forceOkTxt s = s |> FieldDataTypes.Text.createShort |> forceOk
        let forceOkTxtFirstWord (s:string) = (s.Split(" ").[0] |> FieldDataTypes.Text.createShort |> forceOk)
        let currentWfoId = name.About.Replace("https://list.worldfloraonline.org/", "")
        match name.Rank.Resource with
        | "https://list.worldfloraonline.org/terms/subspecies"
        | "https://list.worldfloraonline.org/terms/variety" ->
            printfn "Skipping variety / spp. - TODO"
            tree
        | "https://list.worldfloraonline.org/terms/species" ->
            let genus = Population.Taxonomy.Genus(name.GenusName.Value |> forceOkTxt)
            let matchedParent =
                (matchQueryString genus
                |> WFOMatchService.Load).Match
                |> Option.map(fun m -> m.WfoId)
                |> Option.map(fun wfoId -> stableCache.Load (sprintf "https://list.worldfloraonline.org/sw_data.php?wfo=%s&format=rdfxml" wfoId))
            let tree =
                (Population.Taxonomy.Species(
                    name.FullName.Split(" ").[0] |> forceOkTxt,
                    name.FullName.Split(" ").[1] |> forceOkTxt,
                    name.Authorship.Value |> forceOkTxt), currentWfoId) :: tree
            match matchedParent with
            | Some m -> heirarchyFor' m.TaxonConcept.IsPartOf m.TaxonConcept.HasName.TaxonName tree
            | None -> tree
        | _ ->
            let thisTaxon =
                match name.Rank.Resource with
                | "https://list.worldfloraonline.org/terms/code" -> Some <| Population.Taxonomy.Life
                | "https://list.worldfloraonline.org/terms/genus" -> Some <| Population.Taxonomy.Genus(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/family" -> Some <| Population.Taxonomy.Family(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/order" -> Some <| Population.Taxonomy.Order(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/phylum" -> Some <| Population.Taxonomy.Phylum(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/class" -> Some <| Population.Taxonomy.Class(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/kingdom" -> Some <| Population.Taxonomy.Kingdom(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/subfamily" -> Some <| Population.Taxonomy.Subfamily(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/tribe" -> Some <| Population.Taxonomy.Tribe(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/subtribe" -> Some <| Population.Taxonomy.Subtribe(name.FullName |> forceOkTxtFirstWord)
                | "https://list.worldfloraonline.org/terms/subkingdom"
                | "https://list.worldfloraonline.org/terms/subclass"
                | "https://list.worldfloraonline.org/terms/subgenus"
                | "https://list.worldfloraonline.org/terms/superorder"
                | "https://list.worldfloraonline.org/terms/suborder" 
                | "https://list.worldfloraonline.org/terms/supertribe" 
                | "https://list.worldfloraonline.org/terms/section" 
                | "https://list.worldfloraonline.org/terms/subsection" 
                | "https://list.worldfloraonline.org/terms/series" 
                | "https://list.worldfloraonline.org/terms/unranked"
                | "https://list.worldfloraonline.org/terms/subseries" -> None
                // | _ -> failwithf "Unknown rank %s" name.Rank.Resource
                | _ -> printfn "Unknown rank %s" name.Rank.Resource; None
            let tree =
                match thisTaxon with
                | None -> tree
                | Some t -> (t, currentWfoId) :: tree
            match parent with
            | Some p ->
                let wfoId = p.Resource.Replace("https://list.worldfloraonline.org/", "")
                // printfn "Accessing [%s]..." (sprintf "https://list.worldfloraonline.org/sw_data.php?wfo=%s&format=rdfxml" wfoId)
                let parentData = stableCache.Load (sprintf "https://list.worldfloraonline.org/sw_data.php?wfo=%s&format=rdfxml" wfoId)
                heirarchyFor' parentData.TaxonConcept.IsPartOf parentData.TaxonConcept.HasName.TaxonName tree
            | None -> tree

    let heirarchyFor wfoId =
        let data = stableCache.Load (sprintf "https://list.worldfloraonline.org/sw_data.php?wfo=%s&format=rdfxml" wfoId)
        heirarchyFor' data.TaxonConcept.IsPartOf data.TaxonConcept.HasName.TaxonName []

    let tryMatch (query:string) =
        try
            let result = WFOMatchService.Load query
            result.Match
            |> Option.map(fun m ->
                let heirarchy = heirarchyFor m.WfoId |> List.rev
                heirarchy.Head, heirarchy.Tail
            )
        with e ->
            printfn "Problem parsing JSON server result at %s ! Skipping." query
            None

    let candidates (query:string) =
        try
            let result = WFOMatchService.Load query
            result.Candidates
            |> Seq.truncate 5
            |> Seq.choose(fun c ->
                let heirarchy = heirarchyFor c.WfoId |> List.rev
                if heirarchy.Length = 0
                then
                    printfn "Problem with candidate? %A" c
                    None
                else Some (heirarchy.Head, heirarchy.Tail))
        with e ->
            printfn "Problem parsing JSON server result at %s ! Skipping." query
            []

    type WorldFloraResult = {
        OriginalId: string
        PreferredUsageId: string
        WfoReleaseYear: int
        WfoReleaseMonth: int
        IsASynonymOf: WorldFloraResult list
        LatinName: option<Population.Taxonomy.TaxonNode>
    }

    /// Return the latest version of the WFO taxon and taxa that it
    /// is a synonym of (only referring to the same WFO release).
    let rec lookupSynonymsById wfoId =
        printfn "Accessing [%s]..." (sprintf "https://list.worldfloraonline.org/sw_data.php?wfo=%s&format=rdfxml" wfoId)
        let data = stableCache.Load (sprintf "https://list.worldfloraonline.org/sw_data.php?wfo=%s&format=rdfxml" wfoId)
        let currentAcceptedWfoId = data.TaxonConcept.HasName.TaxonName.CurrentPreferredUsage.Resource.Replace("https://list.worldfloraonline.org/", "")
        let wfoRelease = data.TaxonConcept.About.Substring(data.TaxonConcept.About.Length - 7, 7)
        let synonyms =
            match data.TaxonConcept.HasName.TaxonName.IsSynonymOfs |> Seq.isEmpty with
            | true -> []
            | false ->
                data.TaxonConcept.HasName.TaxonName.IsSynonymOfs
                |> Seq.filter(fun x -> x.Resource.EndsWith wfoRelease)
                |> Seq.map(fun x -> x.Resource.Replace("https://list.worldfloraonline.org/", ""))
                |> Seq.toList
                |> List.map(fun identifier -> lookupSynonymsById identifier)

        let name =
            match data.TaxonConcept.HasName.TaxonName.Rank.Resource with
            | "https://list.worldfloraonline.org/terms/species" ->
                Some (Population.Taxonomy.Species(
                    data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[0] |> FieldDataTypes.Text.createShort |> forceOk,
                    data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[1] |> FieldDataTypes.Text.createShort |> forceOk,
                    data.TaxonConcept.HasName.TaxonName.Authorship.Value |> FieldDataTypes.Text.createShort |> forceOk))
            | "https://list.worldfloraonline.org/terms/genus" -> Some (Population.Taxonomy.Genus(data.TaxonConcept.HasName.TaxonName.FullName |> FieldDataTypes.Text.createShort |> forceOk))
            | "https://list.worldfloraonline.org/terms/family" -> Some (Population.Taxonomy.Family(data.TaxonConcept.HasName.TaxonName.FullName |> FieldDataTypes.Text.createShort |> forceOk))
            | "https://list.worldfloraonline.org/terms/tribe" -> Some (Population.Taxonomy.Tribe(data.TaxonConcept.HasName.TaxonName.FullName |> FieldDataTypes.Text.createShort |> forceOk))
            | "https://list.worldfloraonline.org/terms/subtribe" -> Some (Population.Taxonomy.Subtribe(data.TaxonConcept.HasName.TaxonName.FullName |> FieldDataTypes.Text.createShort |> forceOk))
            | "https://list.worldfloraonline.org/terms/subspecies" ->
                match data.TaxonConcept.HasName.TaxonName.Authorship with
                | Some auth ->
                    Some (Population.Taxonomy.Subspecies(
                        data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[0] |> FieldDataTypes.Text.createShort |> forceOk,
                        data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[1] |> FieldDataTypes.Text.createShort |> forceOk,
                        data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[3] |> FieldDataTypes.Text.createShort |> forceOk,
                        auth |> FieldDataTypes.Text.createShort |> forceOk))
                | None ->
                    printfn "Skipping '%s' as it is a nominate subspecies (no authorship detail). Determine authorship manually and amend graph" data.TaxonConcept.HasName.TaxonName.FullName
                    None
            | "https://list.worldfloraonline.org/terms/variety" ->
                match data.TaxonConcept.HasName.TaxonName.Authorship with
                | Some auth ->
                    Some (Population.Taxonomy.Variety(
                        data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[0] |> FieldDataTypes.Text.createShort |> forceOk,
                        data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[1] |> FieldDataTypes.Text.createShort |> forceOk,
                        data.TaxonConcept.HasName.TaxonName.FullName.Split(" ").[3] |> FieldDataTypes.Text.createShort |> forceOk,
                        auth |> FieldDataTypes.Text.createShort |> forceOk))
                | None ->
                    printfn "Skipping '%s' as it is a nominate subspecies (no authorship detail). Determine authorship manually and amend graph" data.TaxonConcept.HasName.TaxonName.FullName
                    None
            | _ -> None

        {
            OriginalId = wfoId
            PreferredUsageId = currentAcceptedWfoId
            WfoReleaseYear = wfoRelease.Substring(0,4) |> int
            WfoReleaseMonth = wfoRelease.Substring(5, 2) |> int
            IsASynonymOf = synonyms
            LatinName = name
        }


module TaxonomyTools =

    let toTaxonomyNode node =
        match node with
        | GraphStructure.PopulationNode p ->
            match p with
            | GraphStructure.TaxonomyNode t -> Some t
            | _ -> None
        | _ -> None

    let rec crawlTaxonomy' directory taxonAtom tree =
        let isA =
            taxonAtom |> snd
            |> Seq.tryPick(fun (_,sink,_,rel) ->
                match rel with
                | GraphStructure.Relation.Population p ->
                    match p with
                    | Population.PopulationRelation.IsA -> Some sink
                    | _ -> None
                | _ -> None
                )
            |> Option.map (Storage.loadAtom directory "PopulationNode")
        match isA with
        | Some is -> is |> Result.bind(fun is -> crawlTaxonomy' directory is ((is |> fst |> snd |> toTaxonomyNode) :: tree))
        | None -> Ok tree

    let crawlTaxonomy directory taxon =
        crawlTaxonomy' directory taxon []

    let isPlant taxonTree =
        taxonTree 
        |> Seq.tryFind(fun t -> t = Population.Taxonomy.Kingdom (FieldDataTypes.Text.createShort "Plantae" |> forceOk))
        |> Option.isSome

    /// Updates the taxa and their links based on the
    /// current WFO results, with the tree ordered from highest
    /// to lowest rank.
    /// Returns bottom-most parent node
    let rec refreshTaxonTree wfoNodeId (higherTaxon: Population.Taxonomy.TaxonNode * string) (restOfTree: list<Population.Taxonomy.TaxonNode * string>) (graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation>) =
        match restOfTree.Length with
        | 0 ->
            let upperNode = higherTaxon |> fst |> GraphStructure.TaxonomyNode |> GraphStructure.Node.PopulationNode
            upperNode, graph
        | _ ->
            printfn "Higher taxon = %A, lower taxon = %A" higherTaxon restOfTree.Head
            // Add lower taxon and link to upper taxon.
            let upperNode = higherTaxon |> fst |> GraphStructure.TaxonomyNode |> GraphStructure.Node.PopulationNode
            let lowerNode = restOfTree.Head |> fst |> GraphStructure.TaxonomyNode |> GraphStructure.Node.PopulationNode
            let existingLowerTaxon =
                Storage.loadAtom graph.Directory "TaxonomyNode" (GraphStructure.makeUniqueKey lowerNode)
            let lowerTaxon, updatedGraph =
                match existingLowerTaxon with
                | Ok p -> p, graph
                | Error _ -> 
                    printfn "Adding new node %A" (fst restOfTree.Head)
                    let r = 
                        Storage.addNodes graph [ restOfTree.Head |> fst |> GraphStructure.TaxonomyNode |> GraphStructure.Node.PopulationNode ]
                        |> forceOk
                    r |> snd |> Seq.head, r |> fst

            // Process synonyms for lower taxon - what is the preferred usage of this name?
            let synonymResult = WorldFloraOnline.lookupSynonymsById (snd restOfTree.Head)
            let synonymNodes =
                synonymResult.IsASynonymOf
                |> List.choose(fun s -> s.LatinName)
                |> List.map(fun t -> t |> GraphStructure.TaxonomyNode |> GraphStructure.Node.PopulationNode)
            let synonymRelations =
                synonymNodes
                |> List.map(fun s ->
                    lowerTaxon |> fst |> fst,
                    s |> GraphStructure.makeUniqueKey,
                    1,
                    (Population.PopulationRelation.IsSynonymOf(
                        "World Flora Online" |> FieldDataTypes.Text.createShort |> forceOk,
                        Time.SimpleDateOnly.TryCreate (Text (sprintf "%i-%i-01" synonymResult.WfoReleaseYear synonymResult.WfoReleaseMonth )) |> Option.get) |> GraphStructure.Relation.Population)
                    )

            let newLowerTaxonRelations =
                lowerTaxon
                |> snd |> List.filter(fun (_,_,_,r) ->
                    match r with
                    | GraphStructure.Relation.Population p ->
                        match p with
                        | Population.PopulationRelation.IsA -> false
                        | Population.PopulationRelation.IsSynonymOf _ -> false
                        | _ -> true
                    | _ -> true )
                |> List.append [
                    lowerTaxon |> fst |> fst,
                    GraphStructure.makeUniqueKey upperNode,
                    1,
                    (Population.PopulationRelation.IsA |> GraphStructure.Relation.Population)
                ]
                |> List.append [ // Add link to WFO
                    lowerTaxon |> fst |> fst,
                    wfoNodeId,
                    1,
                    (Population.PopulationRelation.HasIdentifier (snd restOfTree.Head |> FieldDataTypes.Text.createShort |> forceOk) |> GraphStructure.Relation.Population)
                ]
                |> List.append synonymRelations
                |> List.distinct

            let newGraph =
                Storage.addOrSkipNodes updatedGraph synonymNodes
                |> Result.bind(fun g ->
                    Storage.saveAtom graph.Directory "TaxonomyNode" (lowerTaxon |> fst |> fst) (fst lowerTaxon, newLowerTaxonRelations)
                    |> Result.map(fun _ -> fst g))
                |> Result.forceOk

            refreshTaxonTree wfoNodeId restOfTree.Head restOfTree.Tail newGraph


    let updateTaxon wfoNodeId (graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation>) atom =
        let taxon = atom |> fst |> snd |> toTaxonomyNode
        let tree =
            atom 
            |> crawlTaxonomy graph.Directory
            |> Result.bind(fun l ->
                if l |> List.contains None then Error (sprintf "Taxonomy was corrupted (%A / %A)" taxon l)
                else l |> List.choose id |> List.rev |> Ok)

        let isPlant =
            tree |> Result.map isPlant
        
        match isPlant with
        | Ok isPlant ->
            if isPlant then

                match taxon with
                | None -> Error "Was not a valid taxonomy node when one was expected"
                | Some t ->

                    printfn "Working on taxon node: %A" t

                    let isMatch =
                        WorldFloraOnline.matchQueryString t
                        |> WorldFloraOnline.tryMatch

                    let addMatch m matchWfoId mTree =
                        let trueWfoId = matchWfoId |> FieldDataTypes.Text.createShort |> forceOk
                        printfn "Matched to %s (%A)" trueWfoId.Value m
                        let mTree = (m,matchWfoId) :: mTree
                        match mTree |> List.rev |> List.head |> fst with
                        | Population.Taxonomy.Life ->
                            let newAtom, updatedGraph = refreshTaxonTree wfoNodeId (mTree |> List.rev |> List.head) (mTree |> List.rev |> List.tail) graph
                            printfn "Taxon updated in graph: %A" newAtom
                            Ok updatedGraph
                        | _ ->
                            printfn "There was an incomplete tree for %A. Skipping update." t
                            Ok graph

                    // Found a plant taxon match online.
                    match isMatch with
                    | Some ((m, matchWfoId),mTree) -> addMatch m matchWfoId mTree
                    | None ->
                        printfn "No match on WFO for %A. Looking at candidates..." t
                        let c = WorldFloraOnline.candidates <| WorldFloraOnline.matchQueryString t
                        c |> Seq.iteri(fun i c -> printfn "[%i] %A" i c)
                        printfn "Enter a number or don't to skip."
                        match System.Console.ReadLine() |> Int.tryParse with
                        | Some i ->
                            c |> Seq.tryItem i
                            |> Option.map(fun ((m, matchWfoId), mTree) -> addMatch m matchWfoId mTree)
                            |> Option.defaultValue (Ok graph)
                        | None ->
                            printfn "Skipping."
                            Ok graph
            else
                printfn "Skipping %A (not a plant)" taxon
                Ok graph
        | Error e -> Error e



/// ----
/// Script functions:
/// ----
let run () = result {

    let! graph, wfoAtoms = 
        Storage.loadOrInitGraph Options.dataFolder
        |> Result.bind(fun s -> Storage.addOrSkipNodes s [ GraphStructure.Node.PopulationNode <| GraphStructure.TaxonomicNamesIndexNode {Name = "World Flora Online" |> FieldDataTypes.Text.createShort |> forceOk } ])

    let wfoNodeId = wfoAtoms.Head |> fst |> fst

    let! taxaKeys = graph.Nodes<Population.Taxonomy.TaxonNode>() |> Result.ofOption "Could not load taxonomy nodes"

    let! taxonomyAtoms =
        taxaKeys.Keys
        |> Seq.toList
        |> Storage.loadAtoms graph.Directory "TaxonomyNode"

    let taxonomyAtomsFiltered =
        if Options.skipWfoLinkedTaxa then
            taxonomyAtoms
            |> List.filter(fun t ->
                t |> snd |> Seq.exists(fun (_,_,_,r) ->
                    match r with
                    | GraphStructure.Relation.Population p ->
                        match p with
                        | Population.PopulationRelation.HasIdentifier _ -> true
                        | _ -> false
                    | _ -> false ) |> not)
        else taxonomyAtoms

    let! updatedGraph =
        List.fold (fun s t ->
            s |> Result.bind(fun s -> TaxonomyTools.updateTaxon wfoNodeId s t)) (Ok graph) taxonomyAtomsFiltered

    return updatedGraph

}

printfn "%A" (run ())
System.Console.ReadLine()