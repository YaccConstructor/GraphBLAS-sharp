namespace GraphBLAS.FSharp

type Scalar<'a when 'a : struct and 'a : equality> =
    | Scalar of 'a
    static member op_Implicit (Scalar source) = source
