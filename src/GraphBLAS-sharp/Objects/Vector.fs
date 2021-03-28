namespace GraphBLAS.FSharp

type VectorType =
    | COO
    | Bitmap

type Vector<'a when 'a : struct> =
    | VectorCOO of COOVector<'a>
    | VectorBitmap of BitmapVector<'a>

and COOVector<'a> =
    {
        Size: int
        Indices: int[]
        Values: 'a[]
    }

and BitmapVector<'a> =
    {
        Bitmap: bool[]
        Values: 'a[]
    }

type VectorTuples<'a> =
    {
        Indices: int[]
        Values: 'a[]
    }
