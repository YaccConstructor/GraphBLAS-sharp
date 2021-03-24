namespace GraphBLAS.FSharp

type Vector<'a> =
    | VectorCOO of COOVector<'a>
    | VectorDense of ArrayVector<'a>

and COOVector<'a> =
    {
        Size: int
        Indices: int[]
        Values: 'a[]
    }

and ArrayVector<'a> =
    {
        Values: 'a[]
    }

type VectorTuples<'a> =
    {
        Indices: int[]
        Values: 'a[]
    }
