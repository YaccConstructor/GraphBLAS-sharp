module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Map

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

let mapOptionToValue zero =
    function
    | Some value -> value
    | None -> zero

let makeTest (testContext: TestContext) mapFun zero isEqual (array: 'a option []) =
    if array.Length > 0 then
        let context = testContext.ClContext
        let q = testContext.Queue

        let clArray =
            context.CreateClArrayWithSpecificAllocationMode(DeviceOnly, array)

        let (actualDevice: ClArray<_>) = mapFun q HostInterop clArray

        let actualHost = Array.zeroCreate actualDevice.Length

        q.PostAndReply(fun ch -> Msg.CreateToHostMsg(actualDevice, actualHost, ch))
        |> ignore

        let expected = Array.map (mapOptionToValue zero) array

        "Arrays must be the same"
        |> Utils.compareArrays isEqual actualHost expected

let createTest<'a when 'a: equality> (testContext: TestContext) (zero: 'a) isEqual =

    let context = testContext.ClContext

    let map =
        ClArray.map context wgSize
        <| Map.optionToValueOrZero zero

    makeTest testContext map zero isEqual
    |> testPropertyWithConfig config $"Correctness on {typeof<'a>}"

let testFixtures (testContext: TestContext) =
    [ createTest<int> testContext 0 (=)
      createTest<bool> testContext false (=)

      if Utils.isFloat64Available testContext.ClContext.ClDevice then
          createTest<float> testContext 0.0 Utils.floatIsEqual

      createTest<float32> testContext 0.0f Utils.float32IsEqual
      createTest<byte> testContext 0uy (=) ]

let tests =
    TestCases.gpuTests "ClArray.map tests" testFixtures
