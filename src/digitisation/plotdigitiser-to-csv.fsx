// This script takes a csv file with three columns
// (values, depths, morphotype_name) and converts
// it into a dataset ready for import into the
// graph database.

// Data from plotdigitiser can easily be imported by
// running it through this script.

open System.IO

[<Measure>]
type cm

type HowDepthIsDefined =
    | ByMorphotype of name:string
    | ByLevels of lowest:float<cm> * highest:float<cm> * spacing:float<cm>


// Options that may be changed:
// ----------------------------

/// If there is a taxon that was present at all depths, you may state
/// its name here. This will make it the 'definitive' depth sequence
/// to which all other depths are rounded to. Alternatively, you may
/// specify the depth levels manually by a range and their spacing
/// (e.g. 2cm intervals between X and Y cm).
let definitiveDepths = ByMorphotype "Cyperaceae"
// let definitiveDepths = ByLevels (0.5<cm>, 275.<cm>, 2.<cm>)

/// The file in which the data is currently stored, either absolute
/// or relative to the script.
let filename = "test.csv"


// Script starts here:
// ----------------------------

let fileContents = File.ReadAllText(filename)

type Row = {
    Depth: float<cm>
    Value: float
    Morphotype: string
}

let rows =
    fileContents.Split("\n")
    |> Array.skip 1
    |> Array.map(fun line ->
        
        let cells = line.Split ',' |> Array.map(fun s -> s.Trim())
        {
            Depth = (cells.[1] |> float) * 1.<cm>
            Value = cells.[0] |> float
            Morphotype = cells.[2]
        }
    )

// Figure out what the depths are
let realDepths=
    match definitiveDepths with
    | ByMorphotype definitiveDepthsMorphotype ->
        rows 
        |> Array.filter(fun r -> r.Morphotype = definitiveDepthsMorphotype)
        |> Array.map(fun r -> r.Depth)
        |> Array.sort
    | ByLevels (low, high, spacing) ->
        failwith "not finished"

let withClosestRealDepth =
    rows
    |> Array.map(fun (r) ->
        r,
        realDepths
        |> Array.map(fun realD -> abs (realD - r.Depth), realD)
        |> Array.minBy fst)
    |> Array.groupBy(fun (row,realD) -> row.Morphotype, snd realD)
    |> Array.map(fun (_,rows) ->
        rows 
        |> Array.minBy(fun (r,(diff,realD)) -> diff )
    )
    |> Array.map(fun (r,(diff,realD)) -> r, realD)

// let outputData =
//     withClosestRealDepth
//     |> Array.sortBy(fun (row,_) -> row.Morphotype, row.Depth)
//     |> Array.map(fun (row, closest) -> sprintf "%f,%f,%s" closest row.Value row.Morphotype )

let taxa = rows |> Array.map(fun r -> r.Morphotype) |> Array.distinct

let stacked =
    realDepths
    |> Array.map(fun depth ->
        
        let values =
            taxa
            |> Array.map(fun taxon ->
                withClosestRealDepth
                |> Array.tryFind(fun (row,d2) -> d2 = depth && row.Morphotype = taxon )
                |> Option.map(fun (row, d2) -> row.Value)
                |> Option.defaultValue 0.
                |> fun x -> if x < 0. then 0. else x
            )
        sprintf "%f,%s" depth (values |> Array.map string |> String.concat ",")    
    )
    |> Array.toList

let header = sprintf "depth,%s" (taxa |> String.concat ",")

System.IO.File.WriteAllLines("processed-depth-data.csv", header :: stacked)

