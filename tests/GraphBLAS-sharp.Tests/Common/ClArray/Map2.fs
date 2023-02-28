module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Map2

open Brahma.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Backend.Quotes
open Expecto
open GraphBLAS.FSharp.Backend.Objects.ClContext

let context = defaultContext.Queue

let wgSize = Utils.defaultWorkGroupSize

let config = Utils.defaultConfig

let makeTest<'a when 'a: equality> testContext clMapFun hostMapFun isEqual (leftArray: 'a [], rightArray: 'a []) =
    if leftArray.Length > 0 then
        let context = testContext.ClContext
        let q = testContext.Queue

        let leftClArray =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, leftArray)

        let rightClArray =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, rightArray)

        let (actualDevice: ClArray<'a>) =
            clMapFun q HostInterop leftClArray rightClArray

        let actualHost = Array.zeroCreate actualDevice.Length

        q.PostAndReply(fun ch -> Msg.CreateToHostMsg(actualDevice, actualHost, ch))
        |> ignore

        let expected =
            Array.map2 hostMapFun leftArray rightArray

        "Arrays must be the same"
        |> Utils.compareArrays isEqual actualHost expected

let createTest<'a when 'a: equality> (testContext: TestContext) isEqual hostMapFun mapFunQ =

    let context = testContext.ClContext

    let map = ClArray.map2 context wgSize mapFunQ

    makeTest<'a> testContext map hostMapFun isEqual
    |> testPropertyWithConfig config $"Correctness on {typeof<'a>}"

let testFixturesAdd (testContext: TestContext) =
    [ createTest<int> testContext (=) (+) <@ (+) @>
      createTest<bool> testContext (=) (||) <@ (||) @>

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createTest<float> testContext Utils.floatIsEqual (+) <@ (+) @>

      createTest<float32> testContext Utils.float32IsEqual (+) <@ (+) @>
      createTest<byte> testContext (=) (+) <@ (+) @> ]

let addTests =
    TestCases.gpuTests "ClArray.map2 add tests" testFixturesAdd

let testFixturesMul (testContext: TestContext) =
    [ createTest<int> testContext (=) (*) <@ (*) @>
      createTest<bool> testContext (=) (&&) <@ (&&) @>

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createTest<float> testContext Utils.floatIsEqual (*) <@ (*) @>

      createTest<float32> testContext Utils.float32IsEqual (*) <@ (*) @>
      createTest<byte> testContext (=) (+) <@ (+) @> ]

let mulTests =
    TestCases.gpuTests "ClArray.map2 multiplication tests" testFixturesMul
