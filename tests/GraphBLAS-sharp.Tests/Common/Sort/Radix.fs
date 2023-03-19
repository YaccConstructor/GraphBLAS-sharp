module GraphBLAS.FSharp.Tests.Backend.Common.Sort.Radix

open Expecto
open GraphBLAS.FSharp.Backend.Common.Sort
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions
open GraphBLAS.FSharp.Backend.Objects.ClContext
open Brahma.FSharp

let config = { Utils.defaultConfig with endSize = 10 ; startSize = 2 }

let workGroupSize = Utils.defaultWorkGroupSize

let processor =  Context.defaultContext.Queue

let context = Context.defaultContext.ClContext

let checkResult (inputArray: int []) (actual: int []) =
    let expected = Array.sort inputArray

    "Results must be the same"
    |>Expect.sequenceEqual actual expected

let makeTest sortFun (array: int []) =

    if array.Length > 0 then
        let clArray = array.ToDevice context

        let (clActual: ClArray<int>) = sortFun processor clArray

        checkResult array <| clActual.ToHostAndFree processor

let createTest =

    let sort = Radix.run context workGroupSize

    makeTest sort [| 0; 0; 1; 0; 0; 1; 1; 3; 1|]
    |> testPropertyWithConfig config ""


