module GraphBLAS.FSharp.Tests.Backend.Common.Reduce.ReduceByKey

open GraphBLAS.FSharp.Tests
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Objects.ClContext

let context = Context.defaultContext.ClContext

let processor = Context.defaultContext.Queue

let checkResult (arrayAndKeys: (int * 'a) []) =
    let keys, values =
        Array.sortBy fst arrayAndKeys
        |> Array.unzip


    ()

let makeTest reduce (arrayAndKeys: (int * 'a) []) =
    let keys, values =
        Array.sortBy fst arrayAndKeys
        |> Array.unzip

    if keys.Length > 0 then
        let clKeys =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, keys)

        let clValues =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, values)


        reduce processor clKeys


    ()
