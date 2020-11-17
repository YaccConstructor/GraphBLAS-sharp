namespace GraphBLAS.FSharp

type COORegularMask1D(indices: array<int>, length: int) =
    inherit Mask1D()

    let mutable indices = Array.distinct indices

    override this.Length = Some length

    override this.Item
        with get (idx: int) : bool =
            indices |> Array.exists ( (=) idx)
        and set (idx: int) (mustExist: bool) = failwith "Not implemented"
