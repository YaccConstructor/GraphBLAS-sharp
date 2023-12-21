module GraphBLAS.FSharp.Tests.Backend.Common.ClArray.Choose

open Expecto
open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Tests.Context
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions

let workGroupSize = Constants.Common.defaultWorkGroupSize

let config = Utils.defaultConfig

let makeTest<'a, 'b> testContext mapFun isEqual choose (array: 'a []) =
    let context = testContext.ClContext
    let q = testContext.Queue

    if array.Length > 0 then

        let clArray = context.CreateClArray array

        let (clResult: ClArray<'b> option) = choose q HostInterop clArray

        let expectedResult = Array.choose mapFun array

        match clResult with
        | Some clResult ->
            let hostResult = clResult.ToHostAndFree testContext.Queue

            "Result should be the same"
            |> Utils.compareArrays isEqual hostResult expectedResult
        | None ->
            "Result must be empty"
            |> Expect.isTrue (expectedResult.Length = 0)

let createTest<'a, 'b> testContext mapFun mapFunQ isEqual =
    ClArray.choose mapFunQ testContext.ClContext workGroupSize
    |> makeTest<'a, 'b> testContext mapFun isEqual
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

let makeTest2 testContext isEqual opMap testFun (firstArray: 'a [], secondArray: 'a []) =
    let context = testContext.ClContext
    let processor = testContext.Queue

    if firstArray.Length > 0 && secondArray.Length > 0 then

        let expected =
            Array.map2 opMap firstArray secondArray
            |> Array.choose id

        if expected.Length > 0 then
            let clFirstArray = context.CreateClArray firstArray
            let clSecondArray = context.CreateClArray secondArray

            let (clActual: ClArray<_>) =
                testFun processor HostInterop clFirstArray clSecondArray

            let actual = clActual.ToHostAndFree processor
            clFirstArray.Free processor
            clSecondArray.Free processor

            "Results must be the same"
            |> Utils.compareArrays isEqual actual expected

let createTest2 testsContext (isEqual: 'a -> 'a -> bool) (opMapQ, opMap) testFun =
    testFun opMapQ testsContext.ClContext Constants.Common.defaultWorkGroupSize
    |> makeTest2 testsContext isEqual opMap
    |> testPropertyWithConfig config $"test on %A{typeof<'a>}"

let testsFixtures2 testContext =
    let context = testContext.ClContext

    [ createTest2 testContext (=) ArithmeticOperations.intAdd ClArray.choose2

      if Utils.isFloat64Available context.ClDevice then
          createTest2 testContext (=) ArithmeticOperations.floatAdd ClArray.choose2

      createTest2 testContext (=) ArithmeticOperations.float32Add ClArray.choose2
      createTest2 testContext (=) ArithmeticOperations.boolAdd ClArray.choose2 ]

let tests2 =
    TestCases.gpuTests "choose2 add" testsFixtures2

let allTests = testList "Choose" [ tests; tests2 ]
