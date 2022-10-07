namespace GraphBLAS.FSharp.Backend

module DenseVector =
    let zeroCreate<'a when 'a : struct> (size: int) : Vector<'a> =
            DenseVector.FromArray (Array.zeroCreate size, fun _ -> true)
            |> VectorDense

    let ofList (elements: (int * 'a) list) (isZero: 'a -> bool) : Vector<'a> =
        let (indices, values) =
            elements
            |> Array.ofList
            |> Array.sortBy fst
            |> Array.unzip

        (values, isZero)
        |> DenseVector.FromArray
        |> VectorDense
