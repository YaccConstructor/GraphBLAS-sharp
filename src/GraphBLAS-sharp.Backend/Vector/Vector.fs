namespace GraphBLAS.FSharp.Backend

module Vector =
    let zeroCreate<'a when 'a : struct> (format: VectorFormat) (size: int) : Vector<'a> =
        match format with
        | COO ->
            VectorCOO
            <| COOVector.FromTuples(size, [||], [||])
        | Dense ->
            Array.zeroCreate size
            |> DenseVector.FromArray
            |> VectorDense

    let ofList (format: VectorFormat) (size: int) (elements: (int * 'a) list) : Vector<'a> =
        let (indices, values) =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        match format with
        | COO ->
            VectorCOO
            <| COOVector.FromTuples(size, indices, values)
        | Dense ->
            values
            |> DenseVector.FromArray
            |> VectorDense







