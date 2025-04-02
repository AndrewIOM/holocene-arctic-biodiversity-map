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
            "Armeria scabra", [ Subspecies(txt "Armeria", txt "scabra", txt "sibirica", txt "(Turcz.) Hyl.") ]
            "Betula nana", [ Species(txt "Betula", txt "nana", txt "L.") ]
            "Campanula uniflora", [ Species(txt "Campanula", txt "uniflora", txt "L.") ]
            "Cassiope tetragona", [ Species(txt "Cassiope", txt "tetragona", txt "(L.) D. Don.") ]
            // "Carex arctogena", []
            "Carex bigelowii", [ Species(txt "Carex", txt "bigelowii", txt "Torr.") ]
            "Carex brunescens", [ Species(txt "Carex", txt "brunescens", txt "(Pers.) Poir.") ]
            "Carex capillaris", [ Species(txt "Carex", txt "capillaris", txt "L.") ]
            "Carex gynocrates", [ Species(txt "Carex", txt "gynocrates", txt "Wormsk.") ]
            "Carex lachenalii", [ Species(txt "Carex", txt "lachenalii", txt "Schkuhr.") ]
            "Carex misandra", [ Species(txt "Carex", txt "misandra", txt "R.Br.") ]
            "Carex nardina", [ Species(txt "Carex", txt "nardina", txt "Fr.") ]
            "Carex norvegica", [ Species(txt "Carex", txt "norvegica", txt "L.") ]
            "Carex rufina", [ Species(txt "Carex", txt "rufina", txt "Drej.") ]
            // "Carex sect. heleonastes", [ Species(txt "", txt "", txt "") ]
            "Carex sp. distigm", [ Genus(txt "Carex") ]
            "Carex sp. distigmitat", [ Genus(txt "Carex") ]
            "Carex sp. tristigm", [ Genus(txt "Carex") ]
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
            "Juncus bi-/triglumis", [ Genus(txt "Juncus") ]
            "Juniperus communis", [ Species(txt "Juniperus", txt "communis", txt "L.") ]
            // "Loiseleuria decumbens", [ Species(txt "", txt "", txt "") ]
            "Luzula sp.", [ Genus(txt "Luzula") ]
            "Minuartia rubella", [ Species(txt "Minuartia", txt "rubella", txt "(Wbg.) Hiern.") ]
            "Papaver radicatum", [ Species(txt "Papaver", txt "radicatum", txt "Rottb.") ]
            "Phippsia algida", [ Species(txt "Phippsia", txt "algida", txt "(Sol.) R. Br.") ]
            "Phyllodoce", [ Genus(txt "Phyllodoce") ]
            "Poa sp.", [ Genus(txt "Poa") ]
            "Polygonum viviparum", [ Species(txt "Polygonum", txt "viviparum", txt "L.") ]
            "Potamogeton pusillus", [ Species(txt "Potamogeton", txt "pusillus", txt "L.") ]
            "Potentilla sp.", [ Genus(txt "Potentilla") ]
            "Ranunculus confervoides", [ Species(txt "Ranunculus", txt "confervoides", txt "(Fr.) Asch. & Graebn.") ]
            "Ranunculus hyperboreus", [ Species(txt "Ranunculus", txt "hyperboreus", txt "Rottb.") ]
            "Salix", [ Genus(txt "Salix") ]
            "Salix arctica", [ Species(txt "Salix", txt "arctica", txt "(Pall.)") ]
            "Salix herbacea", [ Species(txt "Salix", txt "herbacea", txt "L.") ]
            "Saxifraga oppositifolia", [ Species(txt "Saxifraga", txt "oppositifolia", txt "L.") ]
            "Saxifraga caespitosa", [ Species(txt "Saxifraga", txt "caespitosa", txt "L.") ]
            "Silene acaulis", [ Species(txt "Silene", txt "acaulis", txt "(L.) Jacq.") ]
            "Vaccinium uliginosum", [ Species(txt "Vaccinium", txt "uliginosum", txt "L.") ]
            "Armeria", [ Genus(txt "Armeria") ]
            "Cassiope", [ Genus(txt "Cassiope") ]
            "Campanula", [ Genus(txt "Campanula") ]
            "Empetrum", [ Genus(txt "Empetrum") ]
            "Thalictrum", [ Genus(txt "Thalictrum") ]
            "Papaver", [ Genus(txt "Papaver") ]
            "Chenopodiaceae", [ Family(txt "Chenopodiaceae") ]
            "Lycopodium", [ Genus(txt "Lycopodium") ]
            "Saxifraga", [ Genus(txt "Saxifraga") ]
            "Ranunculus", [ Genus(txt "Ranunculus") ]
            "Epilobium", [ Genus(txt "Epilobium") ]
            "Vaccinium", [ Genus(txt "Vaccinium") ]
            "Pedicularis", [ Genus(txt "Pedicularis") ]

            // "Plantago maritima", [ Species(txt "Vaccinium", txt "uliginosum", txt "L.") ]
            "Minuartia groenlandica", [ Species(txt "Minuartia", txt "groenlandica", txt "(Retz.) Ostf.") ]
            "Phyllodoce coerulea", [ Species(txt "Phyllodoce", txt "coerulea", txt "(L.) Bab.") ]
            // "Angelica archangelica", [ Species(txt "Angelica", txt "archangelica", txt "L.") ]
            "Coptis trifolia", [ Species(txt "Coptis", txt "trifolia", txt "(L.) Salisb.") ]
            "Diphasium", [ Genus(txt "Diphasium") ]
            "Bortychium", [ Genus(txt "Bortychium") ]
            "Ledum", [ Genus(txt "Ledum") ]
            "Liguliflorae", [ Genus(txt "Liguliflorae") ]
            "Woodsia", [ Genus(txt "Woodsia") ]
            "Caryophyllaceae", [ Family(txt "Caryophyllaceae") ]
            "Cyperaceae", [ Family(txt "Cyperaceae") ]
            "Cruciferae", [ Family(txt "Cruciferae") ] // Not in latest flora..
            "Poaceae", [ Family(txt "Poaceae") ]
            "Ranunculaceae", [ Family(txt "Ranunculaceae")]
            "Juncaceae", [ Family(txt "Juncaceae")]
            "Alnus crispa", [ Species(txt "Alnus", txt "crispa", txt "(Ait.) Pursh") ]
            "Papaver radicatum", [ Species(txt "Papever", txt "radicatum", txt "Rottb.") ]
            "Cystopteris fragilis subsp. dickieana", [ Subspecies(txt "Cystopteris", txt "fragilis", txt "dickieana", txt "(Sim) Hyl.") ]


        ], Graph.FriendlyKey("inferencemethodnode", "nomenclature_bhjtfog")
