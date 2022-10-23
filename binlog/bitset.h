#ifndef MYSQL_CDC_BITSET_H
#define MYSQL_CDC_BITSET_H

#include <vector>

namespace binlog
{
class BitSet
{
public:
    void Init(size_t size)
    {
        m_bit_set.resize(size, 0);
        m_number_of_set = 0;
    }

    void Set(size_t i)
    {
        if (!m_bit_set[i])
        {
            m_number_of_set++;
            m_bit_set[i] = 1;
        }
    }

    uint8_t Get(size_t i)
    {
        return m_bit_set[i];
    }

    size_t NumberOfSet() const
    {
        return m_number_of_set;
    }

private:
    size_t m_number_of_set = 0;
    std::vector<uint8_t> m_bit_set;
};
}

#endif  // MYSQL_CDC_BITSET_H
