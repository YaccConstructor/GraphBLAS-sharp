namespace GraphBLAS.FSharp

type VectorType =
    | COO
    // | Bitmap

type Vector<'a when 'a : struct> =
    | VectorCOO of COOVector<'a>
    // | VectorBitmap of BitmapVector<'a>

and COOVector<'a> =
    {
        mutable Size: int
        mutable Indices: int[]
        mutable Values: 'a[]
    }

    override this.ToString() =
        [
            sprintf "Sparse Vector\n"
            sprintf "Size:    %i \n" this.Size
            sprintf "Indices: %A \n" this.Indices
            sprintf "Values:  %A \n" this.Values
        ]
        |> String.concat ""

    static member FromTuples(size: int, indices: int[], values: 'a[]) =
        {
            Size = size
            Indices = indices
            Values = values
        }

// and BitmapVector<'a> =
//     {
//         Bitmap: bool[]
//         Values: 'a[]
//     }

type VectorTuples<'a> =
    {
        Indices: int[]
        Values: 'a[]
    }
