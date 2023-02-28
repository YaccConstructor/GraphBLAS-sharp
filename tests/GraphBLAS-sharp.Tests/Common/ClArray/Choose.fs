module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Choose

open GraphBLAS.FSharp.Backend.Common
open Expecto
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend.Objects.ClContext
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes

let workGroupSize = Utils.defaultWorkGroupSize

let config = Utils.defaultConfig

let makeTest<'a, 'b> testContext choose mapFun isEqual (array: 'a []) =
    if array.Length > 0 then
        let context = testContext.ClContext
        let q = testContext.Queue

        let clArray =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, array)

        let (clResult: ClArray<'b>) = choose q HostInterop clArray

        let hostResult = Array.zeroCreate clResult.Length

        q.PostAndReply(fun ch -> Msg.CreateToHostMsg(clResult, hostResult, ch))
        |> ignore

        let expectedResult = Array.choose mapFun array

        "Result should be the same"
        |> Utils.compareArrays isEqual hostResult expectedResult

let createTest<'a, 'b> testContext mapFun mapFunQ isEqual =
    let context = testContext.ClContext

    let choose =
        ClArray.choose context workGroupSize mapFunQ

    makeTest<'a, 'b> testContext choose mapFun isEqual
    |> testPropertyWithConfig config $"Correctness on %A{typeof<'a>} -> %A{typeof<'b>}"

let testFixtures testContext =
    let device = testContext.ClContext.ClDevice

    [ createTest<int option, int> testContext id Map.id (=)
      createTest<byte option, byte> testContext id Map.id (=)
      createTest<bool option, bool> testContext id Map.id (=)

      if Utils.isFloat64Available device then
          createTest<float option, float> testContext id Map.id Utils.floatIsEqual

      createTest<float32 option, float32> testContext id Map.id Utils.float32IsEqual ]

let tests =
    TestCases.gpuTests "ClArray.choose id tests" testFixtures
