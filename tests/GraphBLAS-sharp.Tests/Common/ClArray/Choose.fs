module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Choose

open GraphBLAS.FSharp.Backend.Common
open Expecto
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend.Objects.ClContext
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Backend.Objects.ArraysExtensions

let workGroupSize = Utils.defaultWorkGroupSize

let config = Utils.defaultConfig

let context = Context.defaultContext.ClContext

let processor = defaultContext.Queue

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
    |> testPropertyWithConfig config $"test on %A{typeof<'a>} -> %A{typeof<'b>}"

let testFixtures testContext =
    let device = testContext.ClContext.ClDevice

    [ createTest<int option, int> testContext id Map.id (=)
      createTest<byte option, byte> testContext id Map.id (=)
      createTest<bool option, bool> testContext id Map.id (=)

      if Utils.isFloat64Available device then
          createTest<float option, float> testContext id Map.id Utils.floatIsEqual

      createTest<float32 option, float32> testContext id Map.id Utils.float32IsEqual ]

let tests =
    TestCases.gpuTests "choose id" testFixtures

let makeTest2 isEqual opMap testFun (firstArray: 'a [], secondArray: 'a []) =
    if firstArray.Length > 0
        && secondArray.Length > 0 then

        let expected =
            Array.map2 opMap firstArray secondArray
            |> Array.choose id

        let clFirstArray = context.CreateClArray firstArray
        let clSecondArray = context.CreateClArray secondArray

        let (clActual: ClArray<_>) = testFun processor HostInterop clFirstArray clSecondArray

        let actual = clActual.ToHostAndFree processor
        clFirstArray.Free processor
        clSecondArray.Free processor

        "Results must be the same"
        |> Utils.compareArrays isEqual actual expected

let createTest2 (isEqual: 'a -> 'a -> bool) (opMapQ, opMap) testFun =
    let testFun = testFun context Utils.defaultWorkGroupSize opMapQ

    makeTest2 isEqual opMap testFun
    |> testPropertyWithConfig { config with maxTest = 1000 } $"test on %A{typeof<'a>}"

let tests2 =
    [ createTest2 (=) ArithmeticOperations.intAdd ClArray.choose2

      if Utils.isFloat64Available context.ClDevice then
        createTest2 (=) ArithmeticOperations.floatAdd ClArray.choose2

      createTest2 (=) ArithmeticOperations.float32Add ClArray.choose2
      createTest2 (=) ArithmeticOperations.boolAdd ClArray.choose2 ]
    |> testList "choose2 add"

let allTests = testList "Choose" [ tests; tests2 ]
