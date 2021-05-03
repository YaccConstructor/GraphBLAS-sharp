namespace GraphBLAS.FSharp.Backend.Common

module internal Utils =
    let defaultWorkGroupSize = 256

    let getDefaultGlobalSize n =
        let m = n - 1
        m - m % defaultWorkGroupSize + defaultWorkGroupSize

    let getValidGlobalSize wgSize neededSize =
        (neededSize + wgSize - 1) / wgSize * wgSize

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
