#include <cstdint>
#include <iostream>
#include <string>

template<typename T>
void HexStr(T* data_source)
{
    uint32_t arr_len = sizeof (T);
    std::string hexstr;
    unsigned char* ptr = (unsigned char*)data_source;
    for (int i = 0; i < arr_len; i++)
    {
        char hex1;
        char hex2;
        int value = ptr[i];
        int v1 = value / 16;
        int v2 = value % 16;

        if (v1 >= 0 && v1 <= 9)
            hex1 = (char)(48 + v1);
        else
            hex1 = (char)(55 + v1);

        if (v2 >= 0 && v2 <= 9)
            hex2 = (char)(48 + v2);
        else
            hex2 = (char)(55 + v2);

        if (hex1 >= 'A' && hex1 <= 'F')
            hex1 += 32;

        if (hex2 >= 'A' && hex2 <= 'F')
            hex2 += 32;
        hexstr = hexstr + hex1 + hex2 + " ";
    }
    std::cout << hexstr << std::endl;
}

uint64_t ToBigEndianInteger(char* ptr, int length)
{
    uint64_t result = 0;
    for (int i = 0; i < length; i++)
    {
        char c = *ptr;
        result = (result << 8) | (c >= 0 ? (int) c : (c+256));
        ptr++;
    }
    return result;
}

template<typename T>
void ToBig(T val, int length)
{
    T val2 = ToBigEndianInteger((char*)&val, length);
    HexStr(&val2);
}

int main()
{
    uint8_t value = 0xAB;
    HexStr(&value);
    ToBig(value, 1);

    uint64_t v2 = 0xABCD;
    HexStr(&v2);
    ToBig(v2, 2);
    ToBig(v2, 3);
    ToBig(v2, 4);
    ToBig(v2, 5);
//    HexStr(&v2);
    return 0;
}