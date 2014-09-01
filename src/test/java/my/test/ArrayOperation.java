package my.test;

/**
 *
 分类：
 1）插入排序（直接插入排序、希尔排序）
 2）交换排序（冒泡排序、快速排序）
 3）选择排序（直接选择排序、堆排序）
 4）归并排序
 5）分配排序（基数排序）
 所需辅助空间最多：归并排序
 所需辅助空间最少：堆排序
 平均速度最快：快速排序
 不稳定：快速排序，希尔排序，堆排序。
 */
public class ArrayOperation {

    //二分查找算法
    public static int branchSearch(int[] array, int searchNum) {
        int low = 0, high = array.length;
        int middle = (high + low) / 2;
        int index = -1;
        if (searchNum < array[0] || searchNum > array[array.length - 1])
            return index;

        while (middle >= 0) {
            if (array[middle] == searchNum) {
                index = middle;
                break;
            }
            if (searchNum > array[middle]) {
                low = middle;
            } else {
                high = middle;
            }
            middle = (low + high) / 2;
        }

        return index;

    }

    // 简单选择排序：在要排序的一组数中，选出最小的一个数与第一个位置的数交换；
    // 然后在剩下的数当中再找最小的与第二个位置的数交换，如此循环到倒数第二个数和最后一个数比较为止。
    public static int[] select_Sort(int[] array) {
        int i, j,  pos; //pos为最小值对应的索引
        for (i = 0; i < array.length; i++) {
            for (j = i + 1,  pos = i; j < array.length; j++) {
                if (array[j] < array[pos]) {
                    pos = j;
                }
            }

            if (pos != i) {
                int temp = array[pos];
                array[pos] = array[i];
                array[i] = temp;
            }
            print(array);
        }
        return array;
    }


    // 快速排序
    public static void quickSort(int a[], int left, int right) {
        int i, j, temp;
        i = left;
        j = right;
        if (left > right)
            return;
        temp = a[left];
        while (i != j)/* 找到最终位置 */
        {
            while (a[j] >= temp && j > i)
                j--;
            if (j > i)
                a[i++] = a[j];
            while (a[i] <= temp && j > i)
                i++;
            if (j > i)
                a[j--] = a[i];

        }
        a[i] = temp;
        quickSort(a, left, i - 1);/* 递归左边 */
        quickSort(a, i + 1, right);/* 递归右边 */
    }

    //希尔排序：算法先将要排序的一组数按某个增量d（n/2,n为要排序数的个数）分成若干组，每组中记录的下标相差d.
    // 对每组中全部元素进行直接插入排序，然后再用一个较小的增量（d/2）对它进行分组，在每组中再进行直接插入排序。
    // 当增量减到1时，进行直接插入排序后，排序完成。
    public static void ShellSort(int[] array) {
        int length = array.length;
        for (int h = length / 2; h > 0; h = h / 2) {
            // here is insert sort
            for (int i = h; i < length; i++) {
                int temp = array[i];
                if (temp < array[i - h]) {
                    for (int j = 0; j < i; j += h) {
                        if (temp < array[j]) {
                            temp = array[j];
                            array[j] = array[i];
                            array[i] = temp;
                        }
                        print(array);
                    }

                }
            }
        }
    }

    // 插入排序:在要排序的一组数中，假设前面(n-1)[n>=2] 个数已经是排好顺序的，现在要把第n个数插到前面的有序数中，
    // 使得这n个数也是排好顺序的。如此反复循环，直到全部排好顺序。
    //http://www.blogjava.net/images/blogjava_net/fancydeepin/Insertion-sort-example-300px.gif
    public static void insert_Sort(int []array){
        int key,j;

        // 从第二个元素开始遍历，一直到最后一个元素
        for(int i =1;i<array.length;i++){
            key=array[i];
            //待排序元素较小
            for (j=i-1; j>=0&&array[j] > key; j--){
                array[j+1] = array[j];
            }
            array[j+1]=key;////插入待排序元素

            print(array);
        }
    }

    // 冒泡排序：在要排序的一组数中，对当前还未排好序的范围内的全部数，自上而下对相邻的两个数依次进行比较和调整，
    // 让较大的数往下沉，较小的往上冒。即：每当两相邻的数比较后发现它们的排序与排序要求相反时，就将它们互换。
    public static void bubble_Sort(int[] array) {
        int temp;
        for(int i=0; i<array.length; i++) {
            for (int j = 0; j <array.length - 1 - i; j++) {//经过一轮之后，最大的值就排到后面去了，所以后面的没必要再比较
                if (array[j] > array[j + 1]) {
                    temp = array[j];
                    array[j] = array[j + 1];
                    array[j + 1] = temp;
                }
                print(array);
            }
        }
    }

    private static void print(int[] array){
        for(int k=0;k<array.length;k++)
            System.out.print(array[k]+" ");

        System.out.println();
    }

    public static void main(String []args){

        int [] c={6, 5, 3, 1, 8, 7, 2,4};

        Integer a=new Integer(3);

        Integer b=new Integer(3);

        System.out.println(a==b);
        //insert_Sort(c);

        //bubble_Sort(c);
        //select_Sort(c);
        //ShellSort(c);
    }
}