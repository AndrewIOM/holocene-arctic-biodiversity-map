#r "../../dist/BiodiversityCoder.Core.dll"
#r "nuget: FSharp.Data"

open BiodiversityCoder.Core
open BiodiversityCoder.Core.Storage
open FSharp.Data

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

// What do we need to do for LiPD?
// - Only take sources that have associated dates ('complete')
// - Get table of individual dates, source info...

// What is lipd?

// - Data tables of key datasets:
// - Zip the file as give extension for lpd for linked palaeo-data.

// bag-info.txt
// bagit.txt
// manifest-md5.txt
// tagmanifest-md5.txt

type LiPD = JsonProvider<"lipd-example/bag/data/metadata.jsonld">

let example = LiPD.Load "lipd-example/bag/data/metadata.jsonld"

// Construct a json file here:

[<Literal>]
let pub = "{ \"Journal\" : \"USGS Open-File Report 2007-1047\", \"title\": \"some title\", \"year\": \"2000\" }"

type Pub = JsonProvider<pub, RootName="pub">

Pub.Pub("Cool", "Cool", 2020)


// Construct a 'BagIt' bag

open System.IO

Directory.CreateDirectory("lipid-file")
Directory.CreateDirectory("lipid-file/data")

File.WriteAllLines("lipid-file/bagit.txt", [
    "BagIt-Version: 1.0"
    "Tag-File-Character-Encoding: UTF-8"
])

let source = 
    match sources.[600] |> fst |> snd with
    | GraphStructure.Node.SourceNode s ->
        match s with
        | Sources.SourceNode.Excluded (s,_,_)
        | Sources.SourceNode.Included (s,_)
        | Sources.SourceNode.Unscreened s -> Ok s
    | _ -> Error "Not a source node"
    |> Result.forceOk

let pubDetails source =
    match source with
    | Sources.Source.Bibliographic b ->
        LiPD.Pub(b.Journal, b.Title, b.Year, [||])

let resolution =
    LiPD.HasResolution(hasMax, hasMean, hasMedian, hasMin)

let columns = [|
    LiPD.Column(tSid, dataMd5, description, number, variableName, hasMaxValue, hasMeanValue, hasMedianValue, hasMinValue, Some resolution, units, measurementMethod, uncertaintyLevel)
|]

// Taxon names - are these columns? Make tables but fill with missing data value?
let tables = [|
    LiPD.MeasurementTable(columns, dataMd5, filename, googleKey, missingValue, tableMd5, tableName)
|]

let chronData =
    [| LiPD.ChronData(tables, models) |]

let lipdVersion = 1.3m
let datasetName = "?" // use timeline guid as ahbdb-...

let geo = LiPD.Geo(geom, "type of geom")

let json = LiPD.Root(
    "context.jsonld",
    "lake sediment",
    chronData, datasetName, geo, "", "",
    lipdVersion, "", "", [| pubDetails source |], "")

json.JsonValue

File.WriteAllText("lipid-file/data/metadata.jsonld", json.JsonValue.AsString())