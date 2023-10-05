namespace GraphBLAS.FSharp.Backend.Algorithms

open Brahma.FSharp
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

module SSSP =
    let run (clContext: ClContext) workGroupSize =

        let less = ArithmeticOperations.less<int>
        let min = ArithmeticOperations.min<int>
        let plus = ArithmeticOperations.intSumAsMul

        let spMVInPlace =
            Operations.SpMVInPlace min plus clContext workGroupSize

        let create = ClArray.create clContext workGroupSize

        let ofList = Vector.ofList clContext workGroupSize

        let eWiseMulLess =
            Vector.map2To less clContext workGroupSize

        let eWiseAddMin =
            Vector.map2To min clContext workGroupSize

        let filter =
            Vector.map2To Mask.op clContext workGroupSize

        let containsNonZero =
            Vector.exists Predicates.isSome clContext workGroupSize

        fun (queue: MailboxProcessor<Msg>) (matrix: ClMatrix<int>) (source: int) ->
            let vertexCount = matrix.RowCount

            //None is System.Int32.MaxValue
            let distance =
                ofList queue DeviceOnly Dense vertexCount [ source, 0 ]

            let mutable front1 =
                ofList queue DeviceOnly Dense vertexCount [ source, 0 ]

            let mutable front2 =
                create queue DeviceOnly vertexCount None
                |> ClVector.Dense

            let mutable stop = false

            while not stop do
                //Getting new frontier
                spMVInPlace queue matrix front1 front2

                //Checking which distances were updated
                eWiseMulLess queue front2 distance front1
                //Updating
                eWiseAddMin queue distance front2 distance

                //Filtering unproductive vertices
                //Front1 is a mask
                filter queue front2 front1 front2

                //Swap fronts
                let temp = front1
                front1 <- front2
                front2 <- temp

                //Checking if no distances were updated
                stop <-
                    not
                    <| (containsNonZero queue front1)
                        .ToHostAndFree(queue)

            front1.Dispose queue
            front2.Dispose queue

            match distance with
            | ClVector.Dense dist -> dist
            | _ -> failwith "not implemented"
