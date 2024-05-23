#r "nuget: Cyjs.NET,0.0.4"
#r "nuget: Elmish,3.1.0"
#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Bolero,0.18.16"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core
open BiodiversityCoder.Core.Storage
open BiodiversityCoder.Core.FieldDataTypes
open Bolero

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

let loadSourcesPointingToOtherSources () : list<Graph.Atom<GraphStructure.Node,GraphStructure.Relation>> =
    Storage.loadIndex directory
    |> Result.forceOk
    |> List.filter(fun i -> i.NodeTypeName = "SourceNode")
    |> List.map(fun i -> i.NodeId)
    |> Storage.loadAtoms directory "SourceNode"
    |> Result.forceOk
    |> List.filter(fun (_,r) -> r |> Seq.exists(fun (a,b,_,_) -> b.AsString.Contains "sourcenode") )

open FSharp.Data

type EndNoteLibrary = CsvProvider<"library.csv">

let library =
    EndNoteLibrary.Load "library.csv"

open BiodiversityCoder.Core.Sources.RecordTypes

let pageNumbers (s:string) =
    if System.Text.RegularExpressions.Regex.IsMatch(s, "(\d{1,5})-(\d{1,5})")
    then
        let m = System.Text.RegularExpressions.Regex.Match(s, "(\d{1,5})-(\d{1,5})")
        Some (int m.Groups.[1].Value, int m.Groups.[2].Value)
    else None

let findInEndNoteLibrary (authorsLastNames:string list) title year =
    let matches = 
        library.Rows
        |> Seq.where(fun x -> x.Title = title && x.Year = year )
        |> Seq.toList
    match matches.Length with
    | 0 -> None
    | 1 -> Some matches.Head
    | _ ->
        // determine most likely match based on authors in the list.
        matches |> Seq.tryFind(fun r -> 
            let thisAuthors = r.Authors.Split("-//-") |> Array.map FieldDataTypes.Author.create |> Array.toList |> Result.ofList
            let thisLastNames = thisAuthors |> Result.lift(fun a -> a |> List.map(fun b -> b.Value.LastName))
            match thisLastNames with
            | Ok lastNames ->
                (authorsLastNames |> Seq.sort) = (lastNames |> Seq.sort)
            | Error _ -> 
                printfn "Error parsing authors list on source"
                false
        )
    |> Option.map(fun r ->
        Sources.Source.Bibliographic
            {
                Author = Text.create (r.Authors.Replace("-//-", "; ")) |> Result.toOption
                Title = Text.create r.Title |> Result.toOption
                Journal = Text.createShort r.Journal |> Result.toOption
                Year = if r.Year.HasValue then Some r.Year.Value else None
                Volume = if r.Volume.Length > 0 then Int.tryParse r.Volume else None
                Number = None
                Pages = pageNumbers r.Pages
                Month = None
                DataAvailability = NotAttachedToSource
            })

let processSource (source:Graph.Atom<GraphStructure.Node,GraphStructure.Relation>) =
    printfn "Source is %s" (source |> fst |> fst).AsString
    result {
        let! sourceType =
            match source |> fst |> snd with
            | GraphStructure.Node.SourceNode s ->
                match s with
                | Sources.SourceNode.Excluded (s,_,_)
                | Sources.SourceNode.Included (s,_)
                | Sources.SourceNode.Unscreened s -> Ok s
            | _ -> Error "Not a source node"

        let textOrEmpty = function
            | Some (s:FieldDataTypes.Text.Text) -> s.Value
            | None -> ""
        let shortTextOrEmpty = function
            | Some (s:FieldDataTypes.Text.ShortText) -> s.Value
            | None -> ""

        let toStringOrEmpty = function
            | Some s -> s.ToString()
            | None -> ""

        let crossRefSearchTerm =
            match sourceType with
            | Sources.Source.GreyLiterature g ->
                Some <| sprintf "%s, %s. %s" g.Contact.LastName.Value g.Contact.FirstName.Value g.Title.Value
            | Sources.Source.Bibliographic b ->
                Some <| sprintf "%s (%s). %s. %s" (textOrEmpty b.Author) (toStringOrEmpty b.Year) (textOrEmpty b.Title) (shortTextOrEmpty b.Journal)
            | _ -> None

        match crossRefSearchTerm with
        | Some term ->
            // Search in crossref for a match.
            let! isMatch = 
                match sourceType with
                | Sources.Source.Bibliographic b ->
                    // For bibliographic, try endnote instead
                    let lastNames = 
                        if b.Author.IsSome then b.Author.Value.Value.Split(";") |> Array.map (fun s -> s.TrimStart().Split(",").[0]) |> Array.toList
                        else []
                    Ok <| findInEndNoteLibrary lastNames (textOrEmpty b.Title) (if b.Year.IsSome then b.Year.Value else 0000)
                | _ -> Sources.CrossRef.tryMatch (System.Web.HttpUtility.UrlEncode term)

            match isMatch with
            | Some m ->
                printfn "Found source: %A" m
                if m = sourceType
                then
                    printfn "Skipping as identical between database and EndNote"
                    return! Ok ()
                else
                    printfn "The old source was: %A" sourceType
                    printfn "Should the source be replaced? [y/n]"
                    let answer = System.Console.ReadLine()
                    match answer with
                    | "y" ->
                        // - Generate new node data and key
                        let nodeToSave =
                            match source |> fst |> snd with
                            | GraphStructure.Node.SourceNode s ->
                                match s with
                                | Sources.SourceNode.Excluded (_,a,b) -> Sources.SourceNode.Excluded (m,a,b)
                                | Sources.SourceNode.Included (_,a) -> Sources.SourceNode.Included (m,a)
                                | Sources.SourceNode.Unscreened _ -> Sources.SourceNode.Unscreened m
                            | _ -> failwith "Impossible"
                            |> GraphStructure.Node.SourceNode
                        
                        let key = GraphStructure.makeUniqueKey nodeToSave
                        
                        // If source already exists (and is not this same node), skip:
                        if (sources |> Seq.exists(fun s -> (s |> fst |> fst) = key)) && key <> (source |> fst |> fst)
                        then 
                            printfn "Node already exists - merge %s with %s" (source |> fst |> fst).AsString key.AsString
                            return! Error "Node already exists"
                        else
                            // - Update relations to use the new key as source
                            let rels =
                                source |> snd
                                |> List.map(fun (source, sink, weight, connData) -> key, sink, weight, connData )

                            // - Construct the new entity to save to file
                            let newAtom : Graph.Atom<GraphStructure.Node,GraphStructure.Relation> = (key, nodeToSave), rels

                            // - Replace atom in file with the new contents, then move to new key location.
                            let oldFileName = (sprintf "atom-%s.json" ((fst source |> fst).AsString.ToLower()))
                            let newFileName = (sprintf "atom-%s.json" (key.AsString.ToLower()))

                            System.IO.File.AppendAllLines("changed-keys.txt", [ sprintf "%s\t%s\t%A\t%A\n" oldFileName newFileName source newAtom ])

                            printfn "Replacing contents of atom file (%s) and moving -> %s" oldFileName newFileName
                            Microsoft.FSharpLu.Json.Compact.serializeToFile (System.IO.Path.Combine(directory, oldFileName)) newAtom
                            
                            match (key <> (source |> fst |> fst)) with
                            | true -> return System.IO.File.Move(System.IO.Path.Combine(directory, oldFileName), System.IO.Path.Combine(directory, newFileName))
                            | false -> return ()

                            // - Replace index entry
                            printfn "Replacing old index entry with a new one"
                            let index = Storage.loadIndex directory |> Result.forceOk
                            let oldIndexEntry = index |> List.find(fun i -> i.NodeId = (source |> fst |> fst))
                            let newIndex = 
                                index |> List.except [ oldIndexEntry ] |> List.append([
                                    {
                                        NodeId = key
                                        PrettyName = (newAtom |> fst |> snd).DisplayName ()
                                        NodeTypeName = "SourceNode"
                                    }])
                            Storage.replaceIndex directory newIndex |> ignore

                            // - Replace sink on outward relations to this node
                            printfn "Scanning for incoming links to the changed node"
                            let sinkSources = loadSourcesPointingToOtherSources ()
                            sinkSources
                            |> List.iter(fun ((k,n),r) ->
                                if r |> Seq.exists(fun (_,sink,_,_) -> sink = ((fst source |> fst)))
                                then
                                    printfn "Found an incoming connection from %s" k.AsString
                                    let rels =
                                        r |> Seq.map(fun (so,sink,w,data) ->
                                            if sink = ((fst source |> fst))
                                            then (so, key, w, data)
                                            else (so, sink, w, data)
                                            )
                                    let atom = (k,n),rels
                                    Storage.makeCacheFile directory (sprintf "atom-%s.json" (k.AsString.ToLower())) atom |> ignore
                                else ()
                                )
                            
                            printfn "Finished source."
                            return! Ok ()
                    | _ ->
                        printfn "Not processing source, as answer was not y"
                        return! Ok ()
            | None -> 
                printfn "No match found. Skipping."
                return! Ok ()
            return Error isMatch
        | None ->
            printfn "Source does not require checking."
            return Ok ()

    }

let grey =
    sources
    |> List.filter(fun s ->
        match s |> fst |> snd with
        | GraphStructure.Node.SourceNode s ->
            match s with
            | Sources.SourceNode.Included (s,_)
            | Sources.SourceNode.Excluded (s,_,_)
            | Sources.SourceNode.Unscreened s ->
                match s with
                | Sources.Source.GreyLiterature _ -> true
                | _ -> false
        | _ -> false )

for g in grey |> Seq.skip 110 do 
    processSource g |> ignore


let biblio =
    sources
    |> List.filter(fun s ->
        match s |> fst |> snd with
        | GraphStructure.Node.SourceNode s ->
            match s with
            | Sources.SourceNode.Included (s,_)
            | Sources.SourceNode.Excluded (s,_,_)
            | Sources.SourceNode.Unscreened s ->
                match s with
                | Sources.Source.Bibliographic _ -> true
                | _ -> false
        | _ -> false )

for g in biblio do 
    processSource g |> ignore

// When year and volume match, and titles are close, safe to assume its a definite match.

let ``check the UsesPrimarySource links are valid`` =
    let sinkSources = loadSourcesPointingToOtherSources ()
    sinkSources
    |> List.iteri(fun i ((k,n),r) ->
        printfn "Source %i" i
        r |> Seq.iter(fun (so,sink,w,data) ->
            let sinkNode = Storage.loadAtom directory "SourceNode" sink
            match sinkNode with
            | Error e -> failwith e
            | Ok sn -> ()
            )
        )
