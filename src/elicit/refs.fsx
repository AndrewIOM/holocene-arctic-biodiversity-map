// -----------------------
// Script to make scenario data from AI extraction
// -----------------------

#r "nuget: Newtonsoft.Json,v=13.0"
#r "nuget: FSharp.Data"
#r "nuget: Microsoft.FSharpLu.Json"
#r "../../dist/BiodiversityCoder.Core.dll"
#load "backing.fsx"

open Backing
open System
open System.Text.RegularExpressions
open BiodiversityCoder.Core

module Settings =

    let sourceIdList = "refs.txt"
    let sourcePdfFolder = "/Users/andrewmartin/Library/CloudStorage/Tresorit-AndrewMartin/CHARTER Work Package 4/Source PDFs/"
    let uploadFolder = "/Users/andrewmartin/Desktop/elicit-upload/"
    let extractionSheetDir = "/Volumes/Server HD/Research Projects/Arctic Biodiversity Map/elicit/extraction-sheets/"
    let graphDir = "/Volumes/Server HD/GitHub Projects/holocene-arctic-biodiversity-map/data"

(**
First, we need to batch copy source PDFs based on their source ID for upload.
*)

module FileOps =

    open System.IO

    let copyPdfsToFolder () =
        let fileNames =
            File.ReadAllLines Settings.sourceIdList
            |> Array.mapi(fun i f ->
                let fileName = f + ".pdf"
                let batchFolder = sprintf "batch_%i" (i / 50)
                let folder = Path.Combine(Settings.uploadFolder, batchFolder)
                printfn "Path %A" folder
                if not (Directory.Exists folder)
                then Directory.CreateDirectory(folder) |> ignore
                {| OldPath = Settings.sourcePdfFolder; NewPath = folder; FileName = fileName |})

        fileNames
        |> Array.iter(fun f ->
            let exists =  File.Exists (f.OldPath + f.FileName)
            if not exists then
                printfn "File doesn't exist: %s" f.FileName
                printfn "Add now and press key when done."
                System.Console.ReadKey() |> ignore
        )

        let filesThatExist =
            fileNames
            |> Seq.filter(fun f ->File.Exists(f.OldPath + f.FileName))
            |> Seq.toList

        filesThatExist
        |> List.map(fun f ->
            if not <| File.Exists (Path.Combine(f.NewPath,f.FileName))
            then File.Copy((Path.Combine(f.OldPath + f.FileName)), Path.Combine(f.NewPath,f.FileName))
        )

printfn "[1] Copy PDFs to upload folder? [y/N]"
if System.Console.ReadKey().Key = ConsoleKey.Y
then
    printfn "Copying source PDFs to upload folder..."
    FileOps.copyPdfsToFolder () |> ignore
    printfn "Done."

(**
Next, we load in the whole extraction sheet and parse it accordingly.
*)

open FSharp.Data
type Extract = CsvProvider<"extract-template.csv", IgnoreErrors=false>

let extracted =
    System.IO.Directory.GetFiles(Settings.extractionSheetDir, "*.csv")
    |> Array.map Extract.Load
    |> Array.map(fun r -> r.Rows)
    |> Seq.concat

let isMarkdownTable (text: string) =
    let pattern = @"^\|(.+?)\|\s*\n"
    let regex = Regex(pattern, RegexOptions.Multiline)
    regex.IsMatch text

let stripMarkdownPreamble (txt:string) =
    txt.Replace("```markdown","").Replace("```","").Trim()

let scenarioList =
    extracted
    |> Seq.map(fun r ->
        
        printfn "File %A" r.Filename
        
        let dateString = r.``Individual dates`` |> stripMarkdownPreamble 
        let depthString = r.``Sample depths`` |> stripMarkdownPreamble 
        let coreString = r.``Sampling context (location)`` |> stripMarkdownPreamble 

        let dates =
            if isMarkdownTable dateString
            then Extract.extractDates dateString
            else []
        let depths =
            if isMarkdownTable depthString
            then Extract.extractDepths depthString
            else []
        let coreLocations =
            if isMarkdownTable coreString
            then Extract.extractCoreLocations depths coreString
            else []

        let okCoreLocations = coreLocations |> List.choose(fun r -> match r with | Ok o -> Some o | _ -> None)            

        let scenarios = scenarios okCoreLocations dates

        let errors =
            coreLocations |> List.choose(fun r ->
                match r with
                | Error e -> Some e
                | Ok _ -> None )

        r.Filename, scenarios, errors
    )
    |> Seq.toList


printfn "Saving results to file..."
Microsoft.FSharpLu.Json.Compact.serializeToFile "scenarios.json" scenarioList
printfn "Done. All operations complete."

printfn "Place completed scenarios into scenarios-checked.json."
printfn "Input scenarios in scenarios-checked.json into graph database? To do so, type 'y'."
if System.Console.ReadKey().Key = ConsoleKey.Y
then

    let scens : list<string * list<BiodiversityCoder.Core.Scenarios.Scenario * list<BiodiversityCoder.Core.Exposure.StudyTimeline.IndividualDateNode>> * list<string>> =
        Microsoft.FSharpLu.Json.Compact.deserializeFile "scenarios-checked.json"

    let graph =
        result {
            let! graph =
                BiodiversityCoder.Core.Storage.loadOrInitGraph Settings.graphDir

            let updated =
                List.fold (fun s t -> s)
                    graph
                    scens

            // Find source node.
            // For each scenario:
                // Input scenario.
                // Find new timeline.
                // Add any dates to the timeline.

            return updated
        }

    if graph.IsError then
        printfn "Errored in graph update: %A" graph

    ()
