namespace GraphBLAS.FSharp

type Scalar<'a when 'a : struct> =
    | Scalar of ArrayScalar<'a>

and ArrayScalar<'a> =
    {
        Value: 'a[]
    }

    static member FromArray(array: 'a[]) =
        {
            Value = array
        }
