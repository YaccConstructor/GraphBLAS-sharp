module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Copy

open Expecto
open Expecto.Logging
open Expecto.Logging.Message
open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Objects.ClContext
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let logger = Log.create "ClArray.Copy.Tests"

let context = Context.defaultContext.ClContext

let wgSize = Utils.defaultWorkGroupSize

let q = Context.defaultContext.Queue

let config = Utils.defaultConfig

let makeTest<'a when 'a: equality> copyFun (array: array<'a>) =
    if array.Length > 0 then
        let clArray = context.CreateClArray array

        let actual = (copyFun q HostInterop clArray: ClArray<_>).ToHostAndFree q
        clArray.Free q

        logger.debug (
            eventX "Actual is {actual}"
            >> setField "actual" $"%A{actual}"
        )

        "Array should be equals to original"
        |> Expect.sequenceEqual actual array

let creatTest<'a when 'a: equality> =
    ClArray.copy context wgSize
    |> makeTest<'a>
    |> testPropertyWithConfig config $"Correctness test on random %A{typeof<'a>} arrays"

let testCases =
    q.Error.Add(fun e -> failwithf "%A" e)

    [ creatTest<int>
      creatTest<bool>

      if Utils.isFloat64Available context.ClDevice then
          creatTest<float>

      creatTest<float32>
      creatTest<byte> ]

let tests =
    testCases |> testList "ClArray.copy tests"
