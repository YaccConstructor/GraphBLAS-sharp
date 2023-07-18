module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Replicate

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Objects.ClContextExtensions
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let logger = Log.create "Replicate.Tests"

let context = Context.defaultContext.ClContext

let q = Context.defaultContext.Queue

let workGroupSize = Utils.defaultWorkGroupSize

let config = Utils.defaultConfig

let makeTest<'a when 'a: equality> replicateFun (array: array<'a>) i =
    if array.Length > 0 && i > 0 then
        let clArray = context.CreateClArray array

        let actual =
            (replicateFun q HostInterop clArray i: ClArray<'a>)
                .ToHostAndFree q

        clArray.Free q

        logger.debug (
            eventX $"Actual is {actual}"
            >> setField "actual" $"%A{actual}"
        )

        let expected =
            array |> Array.replicate i |> Array.concat

        $"Array should contains %i{i} copies of the original one"
        |> Expect.sequenceEqual actual expected

let createTest<'a when 'a: equality> =
    ClArray.replicate context workGroupSize
    |> makeTest<'a>
    |> testPropertyWithConfig config $"Correctness test on random %A{typeof<'a>} arrays"

let testCases =
    q.Error.Add(fun e -> failwithf "%A" e)

    [ createTest<int>
      createTest<bool>

      if Utils.isFloat64Available context.ClDevice then
          createTest<float>

      createTest<float32>
      createTest<byte> ]

let tests =
    testCases |> testList "ClArray.replicate tests"
