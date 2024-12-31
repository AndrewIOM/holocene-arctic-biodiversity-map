// Script to import microfossil atlases or keys
// into the graph database, as defined in the '/data-keys' folder.
// The script will not overwrite previously-added atlases / keys.

#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core
open BiodiversityCoder.Core.Population.BioticProxies
open System.IO
open FSharp.Data

type AtlasDefinition = CsvProvider<"../../data-keys/pollen/pub_fredskild_sitvhogpioshlabd_1973.tsv">
type AtlasMetadata = JsonProvider<"../../data-keys/pollen/pub_fredskild_sitvhogpioshlabd_1973.json">

module Lookup =

    let rank = function
        | "species" -> Rank.Species
        | "subspecies" -> Rank.Subspecies
        | "family" -> Rank.Family
        | "genus" -> Rank.Genus
        | s -> failwithf "Unknown rank: %s" s

    let confidence = function
        | "fully-reliable" -> FullyReliable |> Reliability
        | "fairly-reliable" -> FairlyReliable |> Reliability
        | "unreliable" -> Unreliable |> Reliability
        | "" -> IdentificationConfidence.Unspecified
        | s -> failwithf "Unknown confidence: %s" s


let pollenKeys =
    Directory.GetFiles("../../data-keys/pollen", "*.tsv")
    |> Seq.map(fun path ->
        {| Atlas = AtlasDefinition.Load path
           Metadata = AtlasMetadata.Load (path.Replace(".tsv", ".json"))|})
    |> Seq.toList

let preparedNodes =
    pollenKeys |> List.map(fun atlas ->
        {
            Nomleclature = atlas.Metadata.Nomleclature |> FieldDataTypes.Text.createShort |> Result.forceOk
            Entries = (
                atlas.Atlas.Rows
                |> Seq.map(fun r -> {
                    MorphotypeName = r.Morphotype |> FieldDataTypes.Text.createShort |> Result.forceOk
                    TaxonName = r.Taxon |> FieldDataTypes.Text.createShort |> Result.forceOk
                    Rank = Lookup.rank r.Rank
                    Confidence = Lookup.confidence r.Confidence
                }) |> Seq.toList
            )
            Reference = atlas.Metadata.Reference |> FieldDataTypes.Text.create |> Result.forceOk
        }
        |> Population.BioticProxies.InferenceMethodNode.IdentificationKeyOrAtlasWithLookup
        |> GraphStructure.InferenceMethodNode
        |> GraphStructure.PopulationNode )

// Add atlases if not already in database (no overwrite)

let graph: Storage.FileBasedGraph<GraphStructure.Node,GraphStructure.Relation> =
    Storage.loadOrInitGraph "../../data/"
    |> Result.forceOk

Storage.addOrSkipNodes graph preparedNodes