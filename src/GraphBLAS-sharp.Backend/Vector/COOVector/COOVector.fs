namespace GraphBLAS.FSharp.Backend

module COOVector =
    let zeroCreate<'a when 'a : struct> : Vector<'a> =
        VectorCOO <| COOVector.FromTuples([||], [||])

    let ofList (elements: (int * 'a) list) : Vector<'a> =
        let (indices, values) =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        VectorCOO
        <| COOVector.FromTuples(indices, values)



