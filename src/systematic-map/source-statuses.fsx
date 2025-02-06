#r "nuget: Cyjs.NET,0.0.4"
#r "nuget: Elmish,3.1.0"
#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Bolero,0.18.16"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core
open BiodiversityCoder.Core.Storage

let directory = "data/"
let graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation> =
    Storage.loadOrInitGraph directory
    |> Result.forceOk

let sources: Graph.Atom<GraphStructure.Node,GraphStructure.Relation> list =
    graph.Nodes<Sources.SourceNode> ()
    |> Result.ofOption "No source nodes in database"
    |> Result.lift (Map.toList >> List.map fst)
    |> Result.bind (Storage.loadAtoms graph.Directory "SourceNode")
    |> Result.forceOk

let flattened =
    sources
    |> List.map(fun s ->
        let name = (s |> fst |> snd).DisplayName()
        let nTimelines =
            s |> snd |> Seq.filter(fun (_,_,_,r) ->
                match r with
                | GraphStructure.Relation.Source s ->
                    match s with
                    | Sources.SourceRelation.HasTemporalExtent -> true
                    | _ -> false
                | _ -> false )
            |> Seq.length

        let codingStatToStr = function
            | Sources.CodingProgress.CompletedAll -> "done"
            | Sources.CodingProgress.CompletedNone -> "none"
            | Sources.CodingProgress.InProgress _ -> "in-progress"
            | Sources.CodingProgress.Stalled _ -> "stalled"

        let status, reason, notes, codingStatus, s =
            match s |> fst |> snd with
            | GraphStructure.Node.SourceNode s ->
                match s with
                | Sources.SourceNode.Included (s,c) -> "included", "", "", codingStatToStr c, s
                | Sources.SourceNode.Excluded (s, reason, notes) -> "excluded", reason.ToString(), "", notes.Value, s
                | Sources.SourceNode.Unscreened s -> "unscreened", "", "", "", s
            | _ -> failwith "not a source node"        

        let ofOption (k:FieldDataTypes.Text.Text option) = k |> Option.map(fun k -> k.Value) |> Option.defaultValue ""
        let ofOptionSh (k:FieldDataTypes.Text.ShortText option) = k |> Option.map(fun k -> k.Value) |> Option.defaultValue ""
        let kind, title =
            match s with
            | Sources.Source.Bibliographic b -> "bibliographic", (ofOption b.Title)
            | Sources.Source.DarkData b -> "dark-data", b.Details.Value
            | Sources.Source.DarkDataSource b -> "dark-data-source", (ofOptionSh b.Title)
            | Sources.Source.Database b -> "database", b.FullName.Value
            | Sources.Source.DatabaseEntry b -> "database-entry", (ofOptionSh b.Title)
            | Sources.Source.GreyLiterature b -> "grey-lit", b.Title.Value
            | Sources.Source.GreyLiteratureSource b -> "grey-lit-source", b.Title.Value
            | Sources.Source.PublishedSource b ->
                match b with
                | Sources.Book b -> "book", b.BookTitle.Value
                | Sources.BookChapter b -> "book-chapter", b.ChapterTitle.Value
                | Sources.Dissertation b -> "dissertation", b.Title.Value
                | Sources.IndividualDataset b -> "dataset", b.Title.Value
                | Sources.JournalArticle b -> "journal-article", b.Title.Value

        [ kind; title; status; reason; notes; codingStatus; nTimelines.ToString(); name ]
        |> String.concat "\t" 
    )

System.IO.File.WriteAllLines("index.tsv", flattened)
