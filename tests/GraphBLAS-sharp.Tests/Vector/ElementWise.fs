module Backend.Vector.ElementWise

open Expecto
open Expecto.Logging
open GraphBLAS.FSharp.Backend
open GraphBLAS.FSharp.Tests.Utils
open GraphBLAS.FSharp.Backend.Common
open StandardOperations

let logger =
    Log.create "Vector.ElementWise.Tests"

let context = defaultContext.ClContext

let q = defaultContext.Queue

let config = defaultConfig

let NNZCountCount array isZero =
    Array.filter (fun item -> not <| isZero item) array
    |> Array.length

let checkResult
    isEqual
    resultZero
    (op: 'a -> 'b -> 'c)
    (actual: Vector<'c>)
    (leftArray: 'a [])
    (rightArray: 'b [])
    =

    let expectedArrayLength = leftArray.Length

    let expectedArray =
        Array.create expectedArrayLength resultZero

    for i in 0 .. expectedArrayLength - 1 do
        expectedArray.[i] <- op leftArray.[i] rightArray.[i]

    let (VectorDense expected) = createVectorFromArray Dense expectedArray (isEqual resultZero)

    match actual with
    | VectorDense actual ->
        "arrays must have the same values"
        |> Expect.equal actual expected
    | _ -> failwith "Vector format must be Sparse."

let correctnessGenericTest
    leftIsEqual
    rightIsEqual
    resultIsEqual
    leftZero
    rightZero
    resultZero
    op
    (addFun: MailboxProcessor<_> -> ClVector<'a> -> ClVector<'b> -> ClVector<'c>)
    (leftArray: 'a [], rightArray: 'b [])
    =

    let leftNNZCount =
        NNZCountCount leftArray (leftIsEqual leftZero)

    let rightNNZCount =
        NNZCountCount rightArray (rightIsEqual rightZero)

    if leftNNZCount > 0 && rightNNZCount > 0 then

        let firstVector =
            createVectorFromArray Dense leftArray (leftIsEqual leftZero)

        let secondVector =
            createVectorFromArray Dense rightArray (rightIsEqual rightZero)

        let v1 = firstVector.ToDevice context
        let v2 = secondVector.ToDevice context

        try
            let res = addFun q v1 v2

            v1.Dispose q
            v2.Dispose q

            let actual = res.ToHost q

            res.Dispose q

            checkResult resultIsEqual resultZero op actual leftArray rightArray
        with
        | ex when ex.Message = "InvalidBufferSize" -> ()
        | ex -> raise ex

let addTestFixtures =
    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, Dense"

    let wgSize = 32

    [ let intAddFun =
          Vector.elementWiseAtLeastOne context intSumAtLeastOne wgSize

      correctnessGenericTest (=) (=) (=) 0 0 0 (+) intAddFun
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatAddFun =
          Vector.elementWiseAtLeastOne context floatSumAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (+) floatAddFun
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolAddFun =
          Vector.elementWiseAtLeastOne context boolSumAtLeastOne wgSize

      correctnessGenericTest (=) (=) (=) false false false (||) boolAddFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteAddFun =
          Vector.elementWiseAtLeastOne context byteSumAtLeastOne wgSize

      correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (+) byteAddFun
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let addTests = testList "Backend.Vector.ElementWiseAdd tests" addTestFixtures

let mulTestFixtures =
    let getCorrectnessTestName fstType sndType thrType =
        $"Correctness on AtLeastOne<{fstType}, {sndType}> -> {thrType} option, Dense"

    let wgSize = 32

    [ let intMulFun =
          Vector.elementWiseAtLeastOne context intMulAtLeastOne wgSize

      correctnessGenericTest (=) (=) (=) 0 0 0 (*) intMulFun
      |> testPropertyWithConfig config (getCorrectnessTestName "int" "int" "int")

      let floatMulFun =
          Vector.elementWiseAtLeastOne context floatMulAtLeastOne wgSize

      let fIsEqual =
          fun x y -> abs (x - y) < Accuracy.medium.absolute || x = y

      correctnessGenericTest fIsEqual fIsEqual fIsEqual 0.0 0.0 0.0 (*) floatMulFun
      |> testPropertyWithConfig config (getCorrectnessTestName "float" "float" "float")

      let boolMulFun =
          Vector.elementWiseAtLeastOne context boolMulAtLeastOne wgSize

      correctnessGenericTest (=) (=) (=) false false false (&&) boolMulFun
      |> testPropertyWithConfig config (getCorrectnessTestName "bool" "bool" "bool")

      let byteMulFun =
          Vector.elementWiseAtLeastOne context byteMulAtLeastOne wgSize

      correctnessGenericTest (=) (=) (=) 0uy 0uy 0uy (*) byteMulFun
      |> testPropertyWithConfig config (getCorrectnessTestName "byte" "byte" "byte") ]

let mulTests = testList "Backend.Vector.ElementWiseMul tests" mulTestFixtures
