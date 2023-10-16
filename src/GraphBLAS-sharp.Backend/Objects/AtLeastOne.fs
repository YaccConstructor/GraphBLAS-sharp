namespace GraphBLAS.FSharp.Objects

type AtLeastOne<'a, 'b when 'a: struct and 'b: struct> =
    | Both of 'a * 'b
    | Left of 'a
    | Right of 'b
