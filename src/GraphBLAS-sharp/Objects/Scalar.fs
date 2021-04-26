namespace GraphBLAS.FSharp

type Scalar<'a when 'a : struct> =
    {
        Value: 'a[]
    }

    static member FromArray(array: 'a[]) =
        {
            Value = array
        }
