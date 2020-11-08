namespace GraphBLAS.FSharp

type Scalar<'a> = Scalar of 'a
with
    static member op_Implicit (Scalar source) = source
