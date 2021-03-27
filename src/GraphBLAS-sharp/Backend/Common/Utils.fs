namespace GraphBLAS.FSharp.Backend.Common

module internal Utils =
    let workGroupSize = 256
    let workSize n =
        let m = n - 1
        m - m % workGroupSize + workGroupSize
