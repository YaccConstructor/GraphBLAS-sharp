namespace GraphBLAS.FSharp.Backend.Common

open Brahma.FSharp

module internal Utils =
    let defaultWorkGroupSize = 32

    let floorToPower2 =
        fun x -> x ||| (x >>> 1)
        >> fun x -> x ||| (x >>> 2)
        >> fun x -> x ||| (x >>> 4)
        >> fun x -> x ||| (x >>> 8)
        >> fun x -> x ||| (x >>> 16)
        >> fun x -> x - (x >>> 1)

    let ceilToPower2 =
        fun x -> x - 1
        >> fun x -> x ||| (x >>> 1)
        >> fun x -> x ||| (x >>> 2)
        >> fun x -> x ||| (x >>> 4)
        >> fun x -> x ||| (x >>> 8)
        >> fun x -> x ||| (x >>> 16)
        >> fun x -> x + 1

    let floorToMultiple multiple x = x / multiple * multiple

    let ceilToMultiple multiple x = ((x - 1) / multiple + 1) * multiple

    let getClArrayOfValueTypeSize<'a when 'a: struct> localMemorySize = localMemorySize / sizeof<'a>

    //Option type in C is represented as structure with additional integer field
    let getClArrayOfOptionTypeSize<'a> localMemorySize =
        localMemorySize
        / (sizeof<int> + sizeof<'a>
           |> ceilToMultiple (max sizeof<'a> sizeof<int>))
