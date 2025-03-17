#r "nuget: FSharp.Data,5.0.2"
#r "nuget: Newtonsoft.Json,13.0.3"
#r "nuget: Microsoft.FSharpLu.Json,0.11.7"
#r "../../dist/BiodiversityCoder.Core.dll"

open BiodiversityCoder.Core
open Population.Taxonomy

module Floras =

    /// 1968 edition of the Flora of Greenland. Lookup of macrofossil names as mentioned
    /// in various Fredskild 1970s - 1980s macrofossil diagrams.
    let ``Flora of Greenland (Böcher et al., 1968)`` =
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
        ], Graph.FriendlyKey("inferencemethodnode", "nomenclature_btwhkjktfog")
