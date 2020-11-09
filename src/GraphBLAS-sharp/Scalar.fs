namespace GraphBLAS.FSharp

type Scalar<'a when 'a : struct> = Scalar of 'a
with
    static member op_Implicit (Scalar source) = source
